/*
 * Licensed to Luca Cavanna (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Solr river which allows to index data taken from a running Solr instance
 */
public class SolrRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final ScriptService scriptService;

    private final String solrUrl;

    private final String query;
    private final String[] filterQueries;
    private final String[] fields;
    private final String responseWriterType;
    private final String queryType;
    private final String uniqueKey;
    private final int rows;

    private final String indexName;
    private final String typeName;
    private final String settings;
    private final String mapping;
    private final boolean closeOnCompletion;

    private final String script;
    private final Map<String, Object> scriptParams;
    private final String scriptLang;

    private volatile BulkProcessor bulkProcessor;
    private AtomicInteger start = new AtomicInteger(0);

    static final String DEFAULT_UNIQUE_KEY = "id";


    @Inject
    protected SolrRiver(RiverName riverName, RiverSettings riverSettings, Client client, ScriptService scriptService) {
        super(riverName, riverSettings);
        this.client = client;
        this.scriptService = scriptService;

        this.closeOnCompletion = XContentMapValues.nodeBooleanValue(riverSettings.settings().get("close_on_completion"), true);

        String url = "http://localhost:8983/solr/";
        String q = "*:*";
        String uniqueKey = DEFAULT_UNIQUE_KEY;
        int rows = 10;
        String wt, qt;
        wt = qt = null;
        String[] fq, fl;
        fq = fl = null;
        if (riverSettings.settings().containsKey("solr")) {
            Map<String, Object> solrSettings = (Map<String, Object>) riverSettings.settings().get("solr");
            url = XContentMapValues.nodeStringValue(solrSettings.get("url"), url);
            q = XContentMapValues.nodeStringValue(solrSettings.get("q"), q);
            rows = XContentMapValues.nodeIntegerValue(solrSettings.get("rows"), rows);
            fq = readArrayOrString(solrSettings.get("fq"));
            fl = readArrayOrString(solrSettings.get("fl"));
            wt = XContentMapValues.nodeStringValue(solrSettings.get("wt"), null);
            qt = XContentMapValues.nodeStringValue(solrSettings.get("qt"), null);
            uniqueKey = XContentMapValues.nodeStringValue(solrSettings.get("uniqueKey"), uniqueKey);
        }
        this.solrUrl = url;
        this.query = q;
        this.rows = rows;
        this.uniqueKey = uniqueKey;
        this.filterQueries = fq;
        this.fields = fl;
        this.responseWriterType = wt;
        this.queryType = qt;

        String index = riverName.type();
        String type = "import";
        int maxConcurrentBulk = 10;
        int bulkSize = 100;
        String mapping = null;
        String settings = null;
        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            index = XContentMapValues.nodeStringValue(indexSettings.get("index"), index);
            type = XContentMapValues.nodeStringValue(indexSettings.get("type"), type);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), bulkSize);
            maxConcurrentBulk = XContentMapValues.nodeIntegerValue(indexSettings.get("max_concurrent_bulk"), maxConcurrentBulk);
            settings = XContentMapValues.nodeStringValue(indexSettings.get("settings"), settings);
            mapping = XContentMapValues.nodeStringValue(indexSettings.get("mapping"), mapping);
        }
        this.settings = settings;
        this.mapping = mapping;
        this.indexName = index;
        this.typeName = type;

        String script = null;
        Map<String, Object> scriptParams = Maps.newHashMap();
        String scriptLang = null;
        if (riverSettings.settings().containsKey("transform")) {
            Map<String, Object> transformSettings = (Map<String, Object>) riverSettings.settings().get("transform");
            script = XContentMapValues.nodeStringValue(transformSettings.get("script"), null);
            scriptLang = XContentMapValues.nodeStringValue(transformSettings.get("lang"), null);
            scriptParams = (Map<String, Object>) transformSettings.get("params");
        }
        this.script = script;
        this.scriptParams = scriptParams;
        this.scriptLang = scriptLang;

        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.info("Executed bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
            }
        }).setBulkActions(bulkSize).setConcurrentRequests(maxConcurrentBulk).build();
    }

    private String[] readArrayOrString(Object node) {
        if (XContentMapValues.isArray(node)) {
            List list = (List) node;
            String[] array = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = XContentMapValues.nodeStringValue(list.get(i), null);
            }
            return array;
        }

        String value = XContentMapValues.nodeStringValue(node, null);
        if (value != null) {
            return new String[]{value};
        }
        return null;
    }


    @Override
    public void start() {

        if (!client.admin().indices().prepareExists(indexName).execute().actionGet().exists()) {

            CreateIndexRequestBuilder createIndexRequest = client.admin().indices()
                    .prepareCreate(indexName);

            if (settings != null) {
                createIndexRequest.setSettings(settings);
            }
            if (mapping != null) {
                createIndexRequest.addMapping(typeName, mapping);
            }

            createIndexRequest.execute().actionGet();
        }

        SolrServer solrServer = createSolrServer();
        SolrQuery solrQuery = createSolrQuery();

        Long numFound = null;
        int startParam;
        while ((startParam = start.getAndAdd(rows)) == 0 || startParam < numFound) {
            solrQuery.setStart(startParam);

            try {
                logger.info("Sending query to Solr: {}", solrQuery);
                QueryResponse queryResponse = solrServer.query(solrQuery);
                numFound = queryResponse.getResults().getNumFound();
                if (logger.isWarnEnabled() && numFound == 0) {
                    logger.warn("The solr query {} returned 0 documents", solrQuery);
                }

                SolrDocumentList solrDocumentList = queryResponse.getResults();
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

                for (SolrDocument solrDocument : solrDocumentList) {
                    try {
                        Object idObject = solrDocument.get(uniqueKey);
                        if (idObject instanceof String) {
                            String id = (String) idObject;
                            solrDocument.remove(uniqueKey);
                            StringWriter jsonWriter = new StringWriter();
                            mapper.writeValue(jsonWriter, solrDocument);

                            IndexRequest indexRequest = Requests.indexRequest(indexName).type(typeName).id(id);
                            if (this.script == null) {
                                indexRequest.source(jsonWriter.toString());
                            } else {
                                Tuple<XContentType, Map<String, Object>> newSourceAndContent = transformDocument(jsonWriter.toString());
                                indexRequest.source(newSourceAndContent.v2(), newSourceAndContent.v1());
                            }

                            bulkProcessor.add(indexRequest);
                        } else {
                            if (idObject == null) {
                                logger.error("The uniqueKey value is null");
                            } else {
                                logger.error("The uniqueKey value is not a string but a {}", idObject.getClass().getName());
                            }
                        }
                    } catch (IOException e) {
                        logger.warn("Error while importing documents from solr to elasticsearch", e);
                    }
                }
            } catch (SolrServerException e) {
                logger.error("Error while executing the solr query [" + solrQuery.getQuery() + "]", e);
                //if a query fails the next ones are most likely going to fail too
                return;
            }
        }

        bulkProcessor.close();

        logger.info("Data import from solr to elasticsearch completed");

        if (closeOnCompletion) {
            logger.info("Deleting river");
            client.admin().indices().prepareDeleteMapping("_river").setType(riverName.name()).execute();
        }
    }

    protected Tuple<XContentType, Map<String, Object>> transformDocument(String jsonDocument) {
        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(jsonDocument.getBytes(Charset.forName("utf-8")), true);

        Map<String, Object> ctx = new HashMap<String, Object>(2);
        ctx.put("_source", sourceAndContent.v2());

        try {
            ExecutableScript executableScript = scriptService.executable(scriptLang, script, scriptParams);
            executableScript.setNextVar("ctx", ctx);
            executableScript.run();
            ctx = (Map<String, Object>) executableScript.unwrap(ctx);
        } catch (Exception e) {
            throw new ElasticSearchIllegalArgumentException("failed to execute script", e);
        }

        final Map<String, Object> updatedSourceAsMap = (Map<String, Object>) ctx.get("_source");
        return new Tuple<XContentType, Map<String, Object>>(sourceAndContent.v1(), updatedSourceAsMap);
    }

    protected SolrServer createSolrServer() {
        SolrServer solrServer = null;
        //Solrj doesn't support json, only javabin or xml
        //We allow to use xml to deal with javabin cross-versions compatibility issues
        if ("xml".equals(responseWriterType)) {
            solrServer = new HttpSolrServer(solrUrl, null, new XMLResponseParser());
        } else {
            solrServer = new HttpSolrServer(solrUrl);
        }
        return solrServer;
    }

    protected SolrQuery createSolrQuery() {
        SolrQuery solrQuery = new SolrQuery(query);
        if (filterQueries != null) {
            solrQuery.setFilterQueries(filterQueries);
        }
        if (fields != null) {
            solrQuery.setFields(fields);
        }
        if (queryType != null) {
            solrQuery.setQueryType(queryType);
        }
        solrQuery.setRows(rows);
        return solrQuery;
    }

    @Override
    public void close() {

    }
}
