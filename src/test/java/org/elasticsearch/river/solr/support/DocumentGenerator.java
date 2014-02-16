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
package org.elasticsearch.river.solr.support;

import org.apache.commons.lang.RandomStringUtils;

import java.util.*;

public class DocumentGenerator {

    private static final int DEFAULT_MAX_NUMBER_OF_DOCUMENTS = 500;

    private final Random random;

    private final List<String> availableKeywords;
    private final List<String> availableCategories;

    public DocumentGenerator(long seed) {
        this.random = new Random(seed);
        availableKeywords = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            availableKeywords.add("keyword" + nextWord());
        }

        availableCategories = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            availableCategories.add("category" + nextWord());
        }
    }

    public Map<String, Map<String, Object>> generateRandomDocuments() {
        return generateDocuments(random.nextInt(DEFAULT_MAX_NUMBER_OF_DOCUMENTS));
    }

    private Map<String, Map<String, Object>> generateDocuments(int count) {
        Map<String, Map<String, Object>> documents = new HashMap<String, Map<String, Object>>();
        for (int i = 1; i <= count; i++) {
            String uniqueKeyFieldValue = "id_" + i;
            documents.put(uniqueKeyFieldValue, generateDocument(uniqueKeyFieldValue));
        }
        return documents;
    }

    public Map<String, Object> generateDocument(String uniqueKeyFieldValue) {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("id", uniqueKeyFieldValue);
        fields.put("id_test", uniqueKeyFieldValue);

        if (random.nextBoolean()) {
            fields.put("title", nextSentence(5));
        }
        if (random.nextBoolean()) {
            fields.put("description", nextSentence(15));
        }
        if (random.nextBoolean()) {
            int numKeywords = random.nextInt(5);
            List<String> keywords = new ArrayList<String>();
            for (int i = 0; i < numKeywords; i++) {
                keywords.add(availableKeywords.get(random.nextInt(availableKeywords.size() - 1)));
            }
            if (!keywords.isEmpty()) {
                fields.put("keywords", keywords);
            }
        }

        if (random.nextBoolean()) {
            fields.put("category", availableCategories.get(random.nextInt(availableCategories.size() - 1)));
        }

        fields.put("publish_date", nextDate());

        return fields;
    }

    public List<String> getAvailableKeywords() {
        return availableKeywords;
    }

    public List<String> getAvailableCategories() {
        return availableCategories;
    }


    public String nextSentence(int numberOfWords) {
        //0 to numberOfWords number of words for each sentence
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < random.nextInt(numberOfWords); i++) {
            builder.append(nextWord()).append(" ");
        }
        return builder.toString();
    }

    public String nextWord() {
        //1 to 10 number of characters for each random word
        return RandomStringUtils.random(random.nextInt(9) + 1, 0, 0, true, false, null, random);
    }

    public Date nextDate() {
        int month = random.nextInt(11) + 1;
        int year = random.nextInt(30) + 1980;
        int date = random.nextInt(365);
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, date);
        return calendar.getTime();
    }
}
