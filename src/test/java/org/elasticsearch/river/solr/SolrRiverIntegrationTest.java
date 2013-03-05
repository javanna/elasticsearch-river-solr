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

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Iterables;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.river.solr.support.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SolrRiverIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(SolrRiverIntegrationTest.class);

    private JettySolrRunner jettySolrRunner;
    private SolrIndexer solrIndexer;
    private final DocumentGenerator documentGenerator = new DocumentGenerator();
    private final RequestExecutor requestExecutor = new RequestExecutor();
    private Node esNode;
    private Client esClient;
    
    private static final File DATA_DIR;
    private static final File SOLR_DATA_DIR;
    private static final File ES_DATA_DIR;
    private static final boolean ES_HTTP_ENABLED = true;

    static {
        String tmpDir = System.getProperty("tempDir", System.getProperty("java.io.tmpdir"));
        if (tmpDir == null) {
            throw new RuntimeException("No system property 'tempDir' or 'java.io.tmpdir' defined");
        }
        DATA_DIR = new File(tmpDir,
                "test-" + SolrRiverIntegrationTest.class.getSimpleName() + "-" + System.currentTimeMillis());
        DATA_DIR.mkdirs();

        SOLR_DATA_DIR = new File(DATA_DIR, "solr");
        ES_DATA_DIR = new File(DATA_DIR, "elasticsearch");
    }

    @BeforeClass
    public void beforeClass() throws Exception {
        //starts Solr server
        File solrHome = new File(Thread.currentThread().getContextClassLoader().getResource("solr/").toURI());
        System.setProperty("solr.solr.home", solrHome.getAbsolutePath());
        System.setProperty("solr.data.dir", SOLR_DATA_DIR.getCanonicalPath());
        jettySolrRunner = new JettySolrRunner("/solr-river", 8983);
        jettySolrRunner.start();
        solrIndexer = new SolrIndexer(new HttpSolrServer("http://localhost:8983/solr-river"));

        //fires elasticsearch node
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("path.data", ES_DATA_DIR.getAbsolutePath())
                .put("http.enabled", ES_HTTP_ENABLED)
                .build();
        esNode = NodeBuilder.nodeBuilder().clusterName("solr-river-test").local(true).settings(settings).build();
        esNode.start();
        esClient = esNode.client();
    }

    @AfterClass
    public void afterClass() throws Exception {
        jettySolrRunner.stop();
        esClient.close();
        esNode.close();
        FileUtils.deleteDirectory(DATA_DIR);
    }

    @BeforeMethod
    public void beforeMethod() throws IOException, SolrServerException {
        //removes data from elasticsearch (both registered river if existing and imported data)
        String[] indices = esClient.admin().cluster().state(new ClusterStateRequest().local(true))
                .actionGet().getState().getMetaData().getConcreteAllIndices();
        esClient.admin().indices().prepareDelete(indices).execute().actionGet();
        //removes data from running solr
        solrIndexer.clearDocuments();
    }

    @Test
    public void testImportDefaultValues() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver();

        checkMultiGetResponse(documentsMap);
        checkMatchAllDocsSearchResponse(documentsMap);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportExistingIndex() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        esClient.admin().indices().prepareCreate("solr").execute().actionGet();

        registerRiver();

        checkMultiGetResponse(documentsMap);
        checkMatchAllDocsSearchResponse(documentsMap);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithRows() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(ImmutableMap.of("rows", 20), null);

        checkMultiGetResponse(documentsMap);
        checkMatchAllDocsSearchResponse(documentsMap);

        checkRiverClosedOnCompletion();

        //for now we test only that the result is the same
        //TODO need to check that the rows param is read (the query sent to Solr actually contains rows=20)
    }

    @Test
    public void testImportWithQuery() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        final String keyword = documentGenerator.getAvailableKeywords().get(0);

        registerRiver(ImmutableMap.of("q", "keywords:" + keyword), null);

        Map<String, Iterable<Field>> expectedDocuments = Maps.filterEntries(documentsMap, new Predicate<Map.Entry<String, Iterable<Field>>>() {
            @Override
            public boolean apply(Map.Entry<String, Iterable<Field>> entry) {
                for (Field field : entry.getValue()) {
                    if ("keywords".equals(field.getName())) {
                        return ((List)field.getValue()).contains(keyword);
                    }
                }
                return false;
            }
        });

        checkMultiGetResponse(expectedDocuments);
        checkMatchAllDocsSearchResponse(expectedDocuments);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWhenNoDocsReturned() throws Exception {
        registerRiver();
        checkMatchAllDocsSearchResponse(Collections.<String, Iterable<Field>>emptyMap());
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithFilterQuery() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        final String keyword = documentGenerator.getAvailableKeywords().get(0);
        final String category = documentGenerator.getAvailableCategories().get(0);
        String[] fq = new String[]{"keywords:" + keyword, "category:" + category};

        registerRiver(ImmutableMap.of("fq", fq), null);

        Map<String, Iterable<Field>> expectedDocuments = Maps.filterEntries(documentsMap, new Predicate<Map.Entry<String, Iterable<Field>>>() {
            @Override
            public boolean apply(Map.Entry<String, Iterable<Field>> entry) {
                boolean keywordMatch = false;
                boolean categoryMatch = false;
                for (Field field : entry.getValue()) {
                    if ("keywords".equals(field.getName())) {
                        keywordMatch = ((List)field.getValue()).contains(keyword);
                    }
                    if ("category".equals(field.getName())) {
                        categoryMatch= category.equals(field.getValue());
                    }
                }
                return categoryMatch && keywordMatch;
            }
        });

        checkMultiGetResponse(expectedDocuments);
        checkMatchAllDocsSearchResponse(expectedDocuments);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithFieldList() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        String[] fl = new String[]{"id", "title", "description"};
        registerRiver(ImmutableMap.of("fl", fl), null);

        final List<String> flAsList = Arrays.asList(fl);
        Map<String, Iterable<Field>> expectedDocuments = Maps.transformValues(documentsMap, new Function<Iterable<Field>, Iterable<Field>>() {
            @Override
            public Iterable<Field> apply(Iterable<Field> fields) {
                return Iterables.filter(fields, new Predicate<Field>() {
                    @Override
                    public boolean apply(Field field) {
                        return flAsList.contains(field.getName());
                    }
                });
            }
        });

        checkMultiGetResponse(expectedDocuments);
        checkMatchAllDocsSearchResponse(documentsMap);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithQueryFilterQueryAndFieldList() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        final String keyword = documentGenerator.getAvailableKeywords().get(0);
        final String category = documentGenerator.getAvailableCategories().get(0);

        String[] fl = new String[]{"id", "title", "description"};

        registerRiver(ImmutableMap.of("q", "keywords:" + keyword,
                                        "fq", "category:" + category,
                                        "fl", fl), null);

        Map<String, Iterable<Field>> expectedDocumentsFiltered = Maps.filterEntries(documentsMap,
                new Predicate<Map.Entry<String, Iterable<Field>>>() {
            @Override
            public boolean apply(Map.Entry<String, Iterable<Field>> entry) {
                boolean keywordMatch = false;
                boolean categoryMatch = false;
                for (Field field : entry.getValue()) {
                    if ("keywords".equals(field.getName())) {
                        keywordMatch = ((List)field.getValue()).contains(keyword);
                    }
                    if ("category".equals(field.getName())) {
                        categoryMatch= category.equals(field.getValue());
                    }
                }
                return categoryMatch && keywordMatch;
            }
        });

        final List<String> flAsList = Arrays.asList(fl);
        Map<String, Iterable<Field>>expectedDocuments = Maps.transformValues(expectedDocumentsFiltered,
                new Function<Iterable<Field>, Iterable<Field>>() {
            @Override
            public Iterable<Field> apply(Iterable<Field> fields) {
                return Iterables.filter(fields, new Predicate<Field>() {
                    @Override
                    public boolean apply(Field field) {
                        return flAsList.contains(field.getName());
                    }
                });
            }
        });

        checkMultiGetResponse(expectedDocuments);
        checkMatchAllDocsSearchResponse(expectedDocuments);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithUniqueKey() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(ImmutableMap.of("uniqueKey", "id_test"), null);

        checkMultiGetResponse(documentsMap, "id_test");
        checkMatchAllDocsSearchResponse(documentsMap);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithMapping() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        String mapping = "{" +
                "    \"import\" : {" +
                "        \"properties\" : {" +
                "            \"keywords\" : {" +
                "                \"type\" : \"multi_field\"," +
                "                \"fields\" : {" +
                "                    \"keywords\" : {\"type\" : \"string\", \"index\" : \"analyzed\"}," +
                "                    \"keywords_stored\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\", \"stored\" : \"true\"}" +
                "                }" +
                "            }" +
                "        }" +
                "    }" +
                "}";

        registerRiver(null, ImmutableMap.of("mapping", mapping));

        checkMultiGetResponse(documentsMap);

        final String keyword = documentGenerator.getAvailableKeywords().get(0);
        QueryBuilder queryBuilder = QueryBuilders.queryString("keywords_stored:" + keyword);
        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch("solr").setQuery(queryBuilder);

        Map<String, Iterable<Field>> expectedDocumentsMap = Maps.filterEntries(documentsMap,
                new Predicate<Map.Entry<String, Iterable<Field>>>() {
                    @Override
                    public boolean apply(Map.Entry<String, Iterable<Field>> entry) {
                        for (Field field : entry.getValue()) {
                            if ("keywords".equals(field.getName())) {
                                return ((List)field.getValue()).contains(keyword);
                            }
                        }
                        return false;
                    }
                });

        checkSearchResponse(expectedDocumentsMap, searchRequestBuilder);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithIndexAndType() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(null, ImmutableMap.of("index", "myindex", "type", "mytype"));

        checkMultiGetResponse(documentsMap, "myindex", "mytype");
        checkMatchAllDocsSearchResponse(documentsMap, "myindex", "mytype");
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithShardsAndReplicas() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        String settings = "{\n" +
                "    \"number_of_shards\" : 1,\n" +
                "    \"number_of_replicas\" : 0\n" +
                "}";

        registerRiver(null, ImmutableMap.of("settings", settings));

        checkMultiGetResponse(documentsMap);
        checkMatchAllDocsSearchResponse(documentsMap);
        //check that number of shards and replicas have been read
        IndexMetaData indexMetaData = esClient.admin().cluster().state(new ClusterStateRequest().local(true))
                .actionGet().getState().metaData().index("solr");
        Assert.assertEquals(indexMetaData.getNumberOfShards(), 1);
        Assert.assertEquals(indexMetaData.getNumberOfReplicas(), 0);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithBulkSize() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(null, ImmutableMap.of("bulk_size", 30, "max_concurrent_bulk", 1));

        checkMultiGetResponse(documentsMap);
        checkMatchAllDocsSearchResponse(documentsMap);
        checkRiverClosedOnCompletion();

        //for now we test only that the result is the same
        //TODO need to check that the bulk_size and max_concurrent_bulk param are actually read
    }

    @Test
    public void testImportNotClosedOnCompletion() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(null, null, ImmutableMap.of("close_on_completion", false));

        checkMultiGetResponse(documentsMap);
        checkMatchAllDocsSearchResponse(documentsMap);
        checkRiverNotClosedOnCompletion();
    }

    @Test
    public void testImportWithScript() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        //running a script that always removes the title field

        Map<String, Iterable<Field>> expectedDocuments = Maps.transformValues(documentsMap, new Function<Iterable<Field>, Iterable<Field>>() {
            @Override
            public Iterable<Field> apply(Iterable<Field> fields) {
                return Iterables.filter(fields, new Predicate<Field>() {
                    @Override
                    public boolean apply(Field field) {
                        return !"title".equals(field.getName());
                    }
                });
            }
        });

        registerRiver(null, null, null, ImmutableMap.of("script", "ctx._source.remove(\"title\")"));

        checkMultiGetResponse(expectedDocuments);
        checkMatchAllDocsSearchResponse(expectedDocuments);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithScriptAndParams() throws Exception {

        Map<String, Iterable<Field>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        //running a script that always removes the title field

        Map<String, Iterable<Field>> expectedDocuments = Maps.transformValues(documentsMap, new Function<Iterable<Field>, Iterable<Field>>() {
            @Override
            public Iterable<Field> apply(Iterable<Field> fields) {
                return Iterables.filter(fields, new Predicate<Field>() {
                    @Override
                    public boolean apply(Field field) {
                        return !"title".equals(field.getName());
                    }
                });
            }
        });

        registerRiver(null, null, null, ImmutableMap.of("script", "ctx._source.remove(fieldName)",
                "params", ImmutableMap.of("fieldName", "title")));

        checkMultiGetResponse(expectedDocuments);
        checkMatchAllDocsSearchResponse(expectedDocuments);
        checkRiverClosedOnCompletion();
    }

    private void checkRiverClosedOnCompletion() {
        checkGetResponseNotExisting("_river", "solr_river", "_meta");
        //we don't want to delete eventual other rivers
        Assert.assertTrue(esClient.admin().indices().prepareExists("_river").execute().actionGet().exists());
    }

    private void checkRiverNotClosedOnCompletion() {
        checkGetResponse("_river", "solr_river", "_meta");
        //we don't want to delete eventual other rivers
        Assert.assertTrue(esClient.admin().indices().prepareExists("_river").execute().actionGet().exists());
    }

    private void checkMultiGetResponse(Map<String, Iterable<Field>> expectedDocumentsMap) {
        checkMultiGetResponse(expectedDocumentsMap, SolrRiver.DEFAULT_UNIQUE_KEY);
    }

    private void checkMultiGetResponse(Map<String, Iterable<Field>> expectedDocumentsMap, String uniqueKeyField) {
        checkMultiGetResponse(expectedDocumentsMap, "solr", "import", uniqueKeyField);
    }

    private void checkMultiGetResponse(Map<String, Iterable<Field>> expectedDocumentsMap, String index, String type) {
        checkMultiGetResponse(expectedDocumentsMap, index, type, SolrRiver.DEFAULT_UNIQUE_KEY);
    }

    private void checkGetResponse(String index, String type, String id) {
        GetRequestBuilder getRequestBuilder = new GetRequestBuilder(esClient).setIndex(index).setType(type).setId(id);
        GetResponse getResponse = requestExecutor.tryExecute(getRequestBuilder, new CheckGetResponseCallback());
        Assert.assertTrue(getResponse.exists());
    }

    private void checkGetResponseNotExisting(String index, String type, String id) {
        GetRequestBuilder getRequestBuilder = new GetRequestBuilder(esClient).setIndex(index).setType(type).setId(id);
        GetResponse getResponse = requestExecutor.tryExecute(getRequestBuilder, new CheckGetResponseNotExistingCallback());
        Assert.assertFalse(getResponse.exists());
    }

    private void checkMultiGetResponse(Map<String, Iterable<Field>> expectedDocumentsMap,
                                       String index, String type, String uniqueKeyField) {

        //each multiget requests a maximum of 10 docs
        Iterable<List<String>> keySets = Iterables.partition(expectedDocumentsMap.keySet(), 10);

        for (List<String> keySet : keySets) {
            MultiGetRequestBuilder multiGetRequestBuilder = new MultiGetRequestBuilder(esClient).add(index, type, keySet);
            //if the response is not ok according to the ResponseCallback we retry for a maximum of 5 seconds
            MultiGetResponse multiGetResponse = requestExecutor.tryExecute(multiGetRequestBuilder, new CheckMultiGetResponseCallback());

            Assert.assertNotNull(multiGetResponse);
            Assert.assertEquals(multiGetResponse.responses().length, keySet.size());

            for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
                Assert.assertFalse(multiGetItemResponse.failed());
                GetResponse getResponse = multiGetItemResponse.response();
                Iterable<Field> expectedDocument = expectedDocumentsMap.get(getResponse.id());
                assertDocumentsEquals(getResponse, expectedDocument, uniqueKeyField);
            }
        }
    }

    private void checkMatchAllDocsSearchResponse(Map<String, Iterable<Field>> expectedDocumentsMap) {
        checkMatchAllDocsSearchResponse(expectedDocumentsMap, "solr", "import");
    }

    private void checkMatchAllDocsSearchResponse(Map<String, Iterable<Field>> expectedDocumentsMap, String index, String type) {
        refreshIndex(index);
        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type);
        SearchResponse searchResponse = search(searchRequestBuilder);
        Assert.assertEquals(searchResponse.hits().totalHits(), expectedDocumentsMap.size());
    }

    private void checkSearchResponse(Map<String, Iterable<Field>> expectedDocumentsMap, SearchRequestBuilder searchRequestBuilder) {
        refreshIndex("solr");
        SearchResponse searchResponse = search(searchRequestBuilder);
        Assert.assertEquals(searchResponse.hits().totalHits(), expectedDocumentsMap.size());
    }

    private void refreshIndex(String index) {
        RefreshRequestBuilder refreshRequestBuilder = esClient.admin().indices().prepareRefresh(index);
        requestExecutor.tryExecute(refreshRequestBuilder, new CheckRefreshResponseCallback());
    }

    private SearchResponse search(SearchRequestBuilder searchRequestBuilder){
        return requestExecutor.tryExecute(searchRequestBuilder, 5000,
                new CheckSearchResponseCallback());
    }

    private static void assertDocumentsEquals(GetResponse getResponse, Iterable<Field> expectedDocument, String uniqueKeyFieldName) {
        Assert.assertTrue(getResponse.exists());
        Map<String, Object> responseMap = getResponse.sourceAsMap();
        Assert.assertNotNull(responseMap);

        int count = 0;
        for (Field field : expectedDocument) {
            //the id is not included in the responseMap and already verified since used as key to retrieve the doc
            if (uniqueKeyFieldName.equals(field.getName())) {
                continue;
            }

            Object actualValue = responseMap.get(field.getName());
            Assert.assertNotNull(actualValue, field.getName() + " field is null");

            Object expectedValue = field.getValue();
            if (expectedValue instanceof Date) {
                //the source contains the date as String, we need to parse it
                Date actualDate = DateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(actualValue.toString()).toDate();
                Assert.assertEquals(actualDate.getTime(), ((Date) expectedValue).getTime());
            } else {
                Assert.assertEquals(actualValue, expectedValue);
            }
            count++;
        }
        //make sure that the response map doesn't have more fields
        Assert.assertEquals(responseMap.size(), count);
    }

    private void registerRiver() throws Exception {
        registerRiver(null, null, null);
    }

    private void registerRiver(Map<String, ? extends Object> solrConfig,
                               Map<String, ? extends Object> indexConfig) throws Exception {
        registerRiver(solrConfig, indexConfig, null);
    }

    private void registerRiver(Map<String, ? extends Object> solrConfig,
                               Map<String, ? extends Object> indexConfig,
                               Map<String, ? extends Object> mainConfig) throws Exception {

        registerRiver(solrConfig, indexConfig, mainConfig, null);
    }

    private void registerRiver(Map<String, ? extends Object> solrConfig,
                               Map<String, ? extends Object> indexConfig,
                               Map<String, ? extends Object> mainConfig,
                               Map<String, ? extends Object> transformConfig) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().startObject();
        builder.field("type", "solr");

        if (mainConfig != null) {
            for (Map.Entry<String, ? extends Object> entry : mainConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        builder.startObject("solr");
        builder.field("url", "http://localhost:8983/solr-river/");
        if (solrConfig != null) {
            for (Map.Entry<String, ? extends Object> entry : solrConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        builder.endObject();

        if (indexConfig != null) {
            builder.startObject("index");
            for (Map.Entry<String, ? extends Object> entry : indexConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (transformConfig != null) {
            builder.startObject("transform");
            for (Map.Entry<String, ? extends Object> entry : transformConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        builder.endObject();

        logger.debug("Registering river \n{}", builder.string());

        esClient.index(Requests.indexRequest("_river").type("solr_river").id("_meta").source(builder)).actionGet();
    }
}