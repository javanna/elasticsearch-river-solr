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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
    private final String requestHandler;
    private final String uniqueKey;
    private final boolean useCursor;
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

    private final ObjectMapper objectMapper;
    private final CloseableHttpClient httpClient;

    static final String DEFAULT_UNIQUE_KEY = "id";
    static final String USE_CURSOR_PARAM = "useCursor";

    @Inject
    @SuppressWarnings("unchecked")
    protected SolrRiver(RiverName riverName, RiverSettings riverSettings, Client client, ScriptService scriptService) {
        super(riverName, riverSettings);
        this.client = client;
        this.scriptService = scriptService;

        this.closeOnCompletion = XContentMapValues.nodeBooleanValue(riverSettings.settings().get("close_on_completion"), true);

        String url = "http://localhost:8983/solr/";
        String q = "*:*";
        String uniqueKey = DEFAULT_UNIQUE_KEY;
        boolean useCursor = false; 
        int rows = 10;
        String qt = "select";
        String[] fq, fl;
        fq = fl = null;
        if (riverSettings.settings().containsKey("solr")) {
            Map<String, Object> solrSettings = (Map<String, Object>) riverSettings.settings().get("solr");
            url = XContentMapValues.nodeStringValue(solrSettings.get("url"), url);
            q = XContentMapValues.nodeStringValue(solrSettings.get("q"), q);
            rows = XContentMapValues.nodeIntegerValue(solrSettings.get("rows"), rows);
            fq = readArrayOrString(solrSettings.get("fq"));
            fl = readArrayOrString(solrSettings.get("fl"));
            qt = XContentMapValues.nodeStringValue(solrSettings.get("qt"), qt);
            uniqueKey = XContentMapValues.nodeStringValue(solrSettings.get("uniqueKey"), uniqueKey);
            useCursor = XContentMapValues.nodeBooleanValue(solrSettings.get(USE_CURSOR_PARAM), useCursor);
        }
        this.solrUrl = url;
        this.query = q;
        this.rows = rows;
        this.uniqueKey = uniqueKey;
        this.useCursor = useCursor;
        this.filterQueries = fq;
        this.fields = fl;
        this.requestHandler = qt;

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

        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        this.httpClient = HttpClients.createDefault();
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

        if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {

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

        StringBuilder baseSolrQuery = createSolrQuery();

        Long numFound = null;
        String cursorMark = "*";
        String nextCursorMark = "";
        boolean finished = false;
        int startParam = start.get();

        while (!finished) {
            String queryPagingParam = useCursor ? 
                    "&cursorMark=" + cursorMark + "&sort=" + uniqueKey + "+asc" 
                    : "&start=" + startParam;
            String solrQuery = baseSolrQuery.toString() + queryPagingParam;
            CloseableHttpResponse httpResponse = null;
            try {
                logger.info("Sending query to Solr: {}", solrQuery);
                httpResponse = httpClient.execute(new HttpGet(solrQuery));

                if (httpResponse.getStatusLine().getStatusCode() != 200) {
                    logger.error("Solr returned non ok status code: {}", httpResponse.getStatusLine().getReasonPhrase());
                    EntityUtils.consume(httpResponse.getEntity());
                    continue;
                }

                JsonNode jsonNode = objectMapper.readTree(EntityUtils.toString(httpResponse.getEntity()));
                JsonNode response = jsonNode.get("response");
                JsonNode numFoundNode = response.get("numFound");
                numFound = numFoundNode.asLong();
                if (useCursor) {
                    JsonNode nextCursorMarkNode = jsonNode
                            .get("nextCursorMark");
                    if (nextCursorMarkNode == null) {
                        throw new RuntimeException(
                                "No nextCursorMark found in SolR response!"
                                        + " Cursor came only with SolR 4.7 and above."
                                        + " You should retry the import after removing the"
                                        + " 'solr." + USE_CURSOR_PARAM
                                        + "' parameter in river declaration.");
                    }
                    nextCursorMark = nextCursorMarkNode.asText();
                }
                
                finished = useCursor ? 
                        cursorMark.equals(nextCursorMark) 
                        : !((startParam = start.addAndGet(rows)) == 0 || startParam < numFound);
                cursorMark = nextCursorMark;
                
                if (logger.isWarnEnabled() && numFound == 0) {
                    logger.warn("The solr query {} returned 0 documents", solrQuery);
                }

                Iterator<JsonNode> docsIterator = response.get("docs").iterator();

                while (docsIterator.hasNext()) {
                    JsonNode docNode = docsIterator.next();

                    try {
                        JsonNode uniqueKeyNode = docNode.get(uniqueKey);

                        if (uniqueKeyNode == null) {
                            logger.error("The uniqueKey value is null");
                        } else {
                            String id = uniqueKeyNode.asText();
                            ((ObjectNode) docNode).remove(uniqueKey);

                            IndexRequest indexRequest = Requests.indexRequest(indexName).type(typeName).id(id);
                            String source = objectMapper.writeValueAsString(docNode);
                            if (this.script == null) {
                                indexRequest.source(source);
                            } else {
                                Tuple<XContentType, Map<String, Object>> newSourceAndContent = transformDocument(source);
                                indexRequest.source(newSourceAndContent.v2(), newSourceAndContent.v1());
                            }

                            bulkProcessor.add(indexRequest);

                        }
                    } catch (IOException e) {
                        logger.warn("Error while importing documents from solr to elasticsearch", e);
                    }
                }
            } catch (IOException e) {
                logger.error("Error while executing the solr query [" + solrQuery + "]", e);
                bulkProcessor.close();
                try {
                    httpClient.close();
                } catch(IOException ioe) {
                    logger.warn(e.getMessage(), ioe);
                }
                //if a query fails the next ones are most likely going to fail too
                return;
            } finally {
                if (httpResponse != null) {
                    try {
                        httpResponse.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }

        bulkProcessor.close();
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
        }

        logger.info("Data import from solr to elasticsearch completed");

        if (closeOnCompletion) {
            logger.info("Deleting river");
            client.admin().indices().prepareDeleteMapping("_river").setType(riverName.name()).execute();
        }
    }

    @SuppressWarnings("unchecked")
    protected Tuple<XContentType, Map<String, Object>> transformDocument(String jsonDocument) {
        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(jsonDocument.getBytes(Charset.forName("utf-8")), true);

        Map<String, Object> ctx = new HashMap<String, Object>(2);
        ctx.put("_source", sourceAndContent.v2());

        try {
            ExecutableScript executableScript = scriptService.executable(scriptLang, script, ScriptService.ScriptType.INLINE, scriptParams);
            executableScript.setNextVar("ctx", ctx);
            executableScript.run();
            ctx = (Map<String, Object>) executableScript.unwrap(ctx);
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("failed to execute script", e);
        }

        final Map<String, Object> updatedSourceAsMap = (Map<String, Object>) ctx.get("_source");
        return new Tuple<XContentType, Map<String, Object>>(sourceAndContent.v1(), updatedSourceAsMap);
    }

    protected StringBuilder createSolrQuery()  {
        StringBuilder queryBuilder = new StringBuilder(solrUrl);

        if (Strings.hasLength(requestHandler)) {
            if (queryBuilder.charAt(queryBuilder.length() - 1) != '/') {
                queryBuilder.append("/");
            }
            queryBuilder.append(requestHandler);
        }

        queryBuilder.append("?q=").append(encode(query)).append("&wt=json");
        if (filterQueries != null) {
            for (String filterQuery : filterQueries) {
                queryBuilder.append("&fq=").append(encode(filterQuery));
            }
        }
        if (fields != null) {
            queryBuilder.append("&fl=");
            for (int i = 0; i < fields.length; i++) {
                if (i>0) {
                    queryBuilder.append(encode(" "));
                }
                queryBuilder.append(encode(fields[i]));
            }
        }

        queryBuilder.append("&rows=").append(rows);
        return queryBuilder;
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
