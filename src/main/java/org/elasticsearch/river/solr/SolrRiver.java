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
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Solr river which allows to index data taken from a running Solr instance
 */
public class SolrRiver extends AbstractRiverComponent implements River {

    private final Client client;

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
    private final int bulkSize;
    private final String settings;
    private final String mapping;

    private volatile BulkRequestBuilder bulkRequest;
    private AtomicInteger start = new AtomicInteger(0);
    private Semaphore bulkSemaphore;

    static final String DEFAULT_UNIQUE_KEY = "id";


    @Inject
    protected SolrRiver(RiverName riverName, RiverSettings riverSettings, Client client) {
        super(riverName, riverSettings);
        this.client = client;

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
        this.bulkSize = bulkSize;
        this.bulkSemaphore = new Semaphore(maxConcurrentBulk);
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

        bulkRequest = client.prepareBulk();

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
                            bulkRequest.add(Requests.indexRequest(indexName).type(typeName).id(id).source(jsonWriter.toString()));
                            if (bulkRequest.numberOfActions() >= bulkSize) {
                                processBulk();
                            }
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

        if (bulkRequest.numberOfActions() > 0) {
            logger.debug("Executing last bulk with {} actions", bulkRequest.numberOfActions());
            processBulk();
        }

        logger.info("Data import from solr to elasticsearch completed");
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

    private void processBulk() {

        try {
            //Depending on max_concurrent_bulk we slow down the data import if there are too many concurrent bulks going on
            logger.debug("Waiting to acquire a permit to execute the bulk");
            bulkSemaphore.acquire();
            logger.debug("Acquired permit to execute the bulk ({} remaining permits)", bulkSemaphore.availablePermits());

            try {
                bulkRequest.execute(new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        bulkSemaphore.release();
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.warn("Failed to execute bulk", e);
                        bulkSemaphore.release();
                    }
                });
            } catch (Exception e) {
                logger.warn("Error executing bulk", e);
                bulkSemaphore.release();
            }

            bulkRequest = client.prepareBulk();

        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

    }

    @Override
    public void close() {

    }
}
