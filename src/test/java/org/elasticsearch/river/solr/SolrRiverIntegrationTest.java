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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
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
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.solr.support.DocumentGenerator;
import org.elasticsearch.river.solr.support.SolrIndexer;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class SolrRiverIntegrationTest extends ElasticsearchIntegrationTest {

    private static JettySolrRunner jettySolrRunner;
    private static SolrIndexer solrIndexer;
    private static DocumentGenerator documentGenerator;
    
    private static final File DATA_DIR;
    private static final File SOLR_DATA_DIR;
    private static final File ES_DATA_DIR;

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

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put("path.data", ES_DATA_DIR).build();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        documentGenerator = new DocumentGenerator(randomLong());
        //starts Solr server
        File solrHome = new File(Thread.currentThread().getContextClassLoader().getResource("solr/").toURI());
        System.setProperty("solr.data.dir", SOLR_DATA_DIR.getCanonicalPath());
        jettySolrRunner = new JettySolrRunner(solrHome.getAbsolutePath(), "/solr-river", 8983);
        jettySolrRunner.start();
        solrIndexer = new SolrIndexer("http://localhost:8983/solr-river");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        jettySolrRunner.stop();
        FileUtils.deleteDirectory(DATA_DIR);
    }

    @Before
    public void wipeData() throws IOException, SolrServerException {
        //removes data from running solr
        solrIndexer.clearDocuments();
    }

    @Test
    public void testImportDefaultValues() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver();

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(documentsMap, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportExistingIndex() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        createIndex("solr");
        ensureGreen("solr");

        registerRiver();

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(documentsMap, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithRows() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(ImmutableMap.of("rows", 20), null);

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(documentsMap, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithQuery() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        final String keyword = documentGenerator.getAvailableKeywords().get(0);

        registerRiver(ImmutableMap.of("q", "keywords:" + keyword), null);

        Map<String, Map<String, Object>> expectedDocuments = Maps.filterEntries(documentsMap, new Predicate<Map.Entry<String, Map<String, Object>>>() {
            @Override
            public boolean apply(Map.Entry<String, Map<String, Object>> entry) {
                for (Map.Entry<String, Object> stringObjectEntry : entry.getValue().entrySet()) {
                    if ("keywords".equals(stringObjectEntry.getKey())) {
                        return ((List)stringObjectEntry.getValue()).contains(keyword);
                    }
                }
                return false;
            }
        });

        ensureIndexingFinished("solr", expectedDocuments.size());
        checkMultiGetResponse(expectedDocuments, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWhenNoDocsReturned() throws Exception {
        registerRiver();
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithFilterQuery() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        final String keyword = documentGenerator.getAvailableKeywords().get(0);
        final String category = documentGenerator.getAvailableCategories().get(0);
        String[] fq = new String[]{"keywords:" + keyword, "category:" + category};

        registerRiver(ImmutableMap.of("fq", fq), null);

        Map<String, Map<String, Object>> expectedDocuments = Maps.filterEntries(documentsMap, new Predicate<Map.Entry<String, Map<String, Object>>>() {
            @Override
            public boolean apply(Map.Entry<String, Map<String, Object>> entry) {
                boolean keywordMatch = false;
                boolean categoryMatch = false;

                for (Map.Entry<String, Object> stringObjectEntry : entry.getValue().entrySet()) {
                    if ("keywords".equals(stringObjectEntry.getKey())) {
                        keywordMatch = ((List) stringObjectEntry.getValue()).contains(keyword);
                    }
                    if ("category".equals(stringObjectEntry.getKey())) {
                        categoryMatch = category.equals(stringObjectEntry.getValue());
                    }
                }
                return categoryMatch && keywordMatch;
            }
        });

        ensureIndexingFinished("solr", expectedDocuments.size());
        checkMultiGetResponse(expectedDocuments, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithFieldList() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        String[] fl = new String[]{"id", "title", "description"};
        registerRiver(ImmutableMap.of("fl", fl), null);

        final List<String> flAsList = Arrays.asList(fl);
        Map<String, Map<String, Object>> expectedDocuments = Maps.transformValues(documentsMap, new Function<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> apply(Map<String, Object> fields) {
                return Maps.filterEntries(fields, new Predicate<Map.Entry<String, Object>>() {
                    @Override
                    public boolean apply(Map.Entry<String, Object> stringObjectEntry) {
                        return flAsList.contains(stringObjectEntry.getKey());
                    }
                });
            }
        });

        ensureIndexingFinished("solr", expectedDocuments.size());
        checkMultiGetResponse(expectedDocuments, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithQueryFilterQueryAndFieldList() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        final String keyword = documentGenerator.getAvailableKeywords().get(0);
        final String category = documentGenerator.getAvailableCategories().get(0);

        String[] fl = new String[]{"id", "title", "description"};

        registerRiver(ImmutableMap.of("q", "keywords:" + keyword,
                                        "fq", "category:" + category,
                                        "fl", fl), null);

        Map<String, Map<String, Object>> expectedDocumentsFiltered = Maps.filterEntries(documentsMap,
                new Predicate<Map.Entry<String, Map<String, Object>>>() {
                    @Override
                    public boolean apply(Map.Entry<String, Map<String, Object>> entry) {
                        boolean keywordMatch = false;
                        boolean categoryMatch = false;
                        for (Map.Entry<String, Object> stringObjectEntry : entry.getValue().entrySet()) {
                            if ("keywords".equals(stringObjectEntry.getKey())) {
                                keywordMatch = ((List) stringObjectEntry.getValue()).contains(keyword);
                            }
                            if ("category".equals(stringObjectEntry.getKey())) {
                                categoryMatch = category.equals(stringObjectEntry.getValue());
                            }
                        }
                        return categoryMatch && keywordMatch;
                    }
                });

        final List<String> flAsList = Arrays.asList(fl);
        Map<String, Map<String, Object>>expectedDocuments = Maps.transformValues(expectedDocumentsFiltered,
                new Function<Map<String, Object>, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> apply(Map<String, Object> fields) {
                        return Maps.filterEntries(fields, new Predicate<Map.Entry<String, Object>>() {
                            @Override
                            public boolean apply(Map.Entry<String, Object> stringObjectEntry) {
                                return flAsList.contains(stringObjectEntry.getKey());
                            }
                        });
                    }
                });

        ensureIndexingFinished("solr", expectedDocuments.size());
        checkMultiGetResponse(expectedDocuments, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithUniqueKey() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(ImmutableMap.of("uniqueKey", "id_test"), null);

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(documentsMap, "solr", "import", "id_test");
        checkRiverClosedOnCompletion();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testImportWithMapping() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        String mapping = "{" +
                "    \"import\" : {" +
                "        \"properties\" : {" +
                "            \"keywords\" : {" +
                "                \"type\" : \"string\"," +
                "                \"fields\" : {" +
                "                    \"keywords_raw\" : {\"type\" : \"string\", \"index\" : \"not_analyzed\"}" +
                "                }" +
                "            }" +
                "        }" +
                "    }" +
                "}";

        registerRiver(null, ImmutableMap.of("mapping", mapping));

        ensureIndexingFinished("solr", documentsMap.size());

        checkMultiGetResponse(documentsMap, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);

        MappingMetaData mappingMetaData = client().admin().indices().prepareGetMappings("solr").get().getMappings().get("solr").get("import");
        Map<String, String> stringObjectMap = (Map<String, String>) ((Map) ((Map) ((Map) mappingMetaData.getSourceAsMap().get("properties"))
                .get("keywords")).get("fields")).get("keywords_raw");
        assertThat(stringObjectMap.get("type"), equalTo("string"));
        assertThat(stringObjectMap.get("index"), equalTo("not_analyzed"));

        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithIndexAndType() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(null, ImmutableMap.of("index", "myindex", "type", "mytype"));

        ensureIndexingFinished("myindex", documentsMap.size());
        checkMultiGetResponse(documentsMap, "myindex", "mytype", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithShardsAndReplicas() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        String settings = "{\n" +
                "    \"number_of_shards\" : 1,\n" +
                "    \"number_of_replicas\" : 0\n" +
                "}";

        registerRiver(null, ImmutableMap.of("settings", settings));

        ensureIndexingFinished("solr", documentsMap.size());

        checkMultiGetResponse(documentsMap, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        //check that number of shards and replicas have been read
        IndexMetaData indexMetaData = client().admin().cluster().prepareState()
                .get().getState().metaData().index("solr");
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(0));
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithBulkSize() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(null, ImmutableMap.of("bulk_size", 30, "max_concurrent_bulk", 1));

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(documentsMap, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();

        //for now we test only that the result is the same
        //TODO need to check that the bulk_size and max_concurrent_bulk param are actually read
    }

    @Test
    public void testImportNotClosedOnCompletion() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        registerRiver(null, null, ImmutableMap.of("close_on_completion", false));

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(documentsMap, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        assertThat(client().prepareGet("_river", "solr_river", "_meta").get().isExists(), equalTo(true));
    }

    @Test
    public void testImportWithScript() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        //running a script that always removes the title field

        Map<String, Map<String, Object>> expectedDocuments = Maps.transformValues(documentsMap, new Function<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> apply(Map<String, Object> fields) {
                return Maps.filterEntries(fields, new Predicate<Map.Entry<String, Object>>() {
                    @Override
                    public boolean apply(Map.Entry<String, Object> stringObjectEntry) {
                        return !"title".equals(stringObjectEntry.getKey());
                    }
                });
            }
        });

        registerRiver(null, null, null, ImmutableMap.of("script", "ctx._source.remove(\"title\")"));

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(expectedDocuments, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    @Test
    public void testImportWithScriptAndParams() throws Exception {

        Map<String, Map<String, Object>> documentsMap = documentGenerator.generateRandomDocuments();
        logger.info("Generated {} documents", documentsMap.size());
        solrIndexer.indexDocuments(documentsMap);
        logger.info("Indexed {} documents in Solr", documentsMap.size());

        //running a script that always removes the title field

        Map<String, Map<String, Object>> expectedDocuments = Maps.transformValues(documentsMap, new Function<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> apply(Map<String, Object> fields) {
                return Maps.filterEntries(fields, new Predicate<Map.Entry<String, Object>>() {
                    @Override
                    public boolean apply(Map.Entry<String, Object> stringObjectEntry) {
                        return !"title".equals(stringObjectEntry.getKey());
                    }
                });
            }
        });

        registerRiver(null, null, null, ImmutableMap.of("script", "ctx._source.remove(fieldName)",
                "params", ImmutableMap.of("fieldName", "title")));

        ensureIndexingFinished("solr", documentsMap.size());
        checkMultiGetResponse(expectedDocuments, "solr", "import", SolrRiver.DEFAULT_UNIQUE_KEY);
        checkRiverClosedOnCompletion();
    }

    private void checkRiverClosedOnCompletion() throws InterruptedException {
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return !client().prepareGet("_river", "solr_river", "_meta").get().isExists();
            }
        }), equalTo(true));
        assertThat(client().admin().indices().prepareExists("_river").get().isExists(), equalTo(true));
    }

    private void checkMultiGetResponse(Map<String, Map<String, Object>> expectedDocumentsMap,
                                       String index, String type, String uniqueKeyField) {

        //each multiget requests a maximum of 100 docs
        Iterable<List<String>> keySets = Iterables.partition(expectedDocumentsMap.keySet(), 100);

        for (List<String> keySet : keySets) {
            MultiGetResponse multiGetResponse = client().prepareMultiGet().add(index, type, keySet).get();
            assertThat(multiGetResponse.getResponses().length, equalTo(keySet.size()));
            for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
                assertThat(multiGetItemResponse.isFailed(), equalTo(false));
                GetResponse getResponse = multiGetItemResponse.getResponse();
                Map<String, Object> stringObjectMap = expectedDocumentsMap.get(getResponse.getId());
                assertDocumentsEquals(getResponse, stringObjectMap, uniqueKeyField);
            }
        }
    }

    private void ensureIndexingFinished(final String index, final long expectedCount) throws InterruptedException {
        ensureGreen(index);
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                client().admin().indices().prepareRefresh(index).get();
                SearchResponse searchResponse = client().prepareSearch(index).setSearchType(SearchType.COUNT).get();
                return searchResponse.getHits().getTotalHits() == expectedCount;
            }
        }), equalTo(true));
    }

    private static void assertDocumentsEquals(GetResponse getResponse, Map<String, Object> expectedDocument, String uniqueKeyFieldName) {
        assertThat(getResponse.isExists(), equalTo(true));
        Map<String, Object> responseMap = getResponse.getSourceAsMap();
        assertThat(responseMap, notNullValue());

        int count = 0;
        for (Map.Entry<String, Object> entry : expectedDocument.entrySet()) {
            //the id is not included in the responseMap and already verified since used as key to retrieve the doc
            if (uniqueKeyFieldName.equals(entry.getKey())) {
                continue;
            }

            Object actualValue = responseMap.get(entry.getKey());
            assertThat(actualValue, notNullValue());

            Object expectedValue = entry.getValue();
            if (expectedValue instanceof Date) {
                //the source contains the date as String, we need to parse it
                Date actualDate = DateFieldMapper.Defaults.DATE_TIME_FORMATTER.parser().parseDateTime(actualValue.toString()).toDate();
                assertThat(actualDate.getTime(), equalTo(((Date) expectedValue).getTime()));
            } else {
                assertThat(actualValue, equalTo(expectedValue));
            }
            count++;
        }
        //make sure that the response map doesn't have more fields
        assertThat(responseMap.size(), equalTo(count));
    }

    private void registerRiver() throws Exception {
        registerRiver(null, null, null);
    }

    private void registerRiver(Map<String, ?> solrConfig,
                               Map<String, ?> indexConfig) throws Exception {
        registerRiver(solrConfig, indexConfig, null);
    }

    private void registerRiver(Map<String, ?> solrConfig,
                               Map<String, ?> indexConfig,
                               Map<String, ?> mainConfig) throws Exception {

        registerRiver(solrConfig, indexConfig, mainConfig, null);
    }

    private void registerRiver(Map<String, ?> solrConfig,
                               Map<String, ?> indexConfig,
                               Map<String, ?> mainConfig,
                               Map<String, ?> transformConfig) throws Exception {

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("type", "solr");

        if (mainConfig != null) {
            for (Map.Entry<String, ?> entry : mainConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        builder.startObject("solr");
        builder.field("url", "http://localhost:8983/solr-river/");
        if (solrConfig != null) {
            for (Map.Entry<String, ?> entry : solrConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        builder.endObject();

        if (indexConfig != null) {
            builder.startObject("index");
            for (Map.Entry<String, ?> entry : indexConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (transformConfig != null) {
            builder.startObject("transform");
            for (Map.Entry<String, ?> entry : transformConfig.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        builder.endObject();

        logger.debug("Registering river \n{}", builder.string());

        client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, "solr_river", "_meta").setSource(builder).get();
    }
}