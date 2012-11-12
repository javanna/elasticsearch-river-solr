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

import java.util.Calendar;
import java.util.Date;

public final class Random {

    private static final java.util.Random RANDOM = new java.util.Random();

    private Random() {

    }

    public static String nextSentence(int numberOfWords) {
        //0 to numberOfWords number of words for each sentence
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < RANDOM.nextInt(numberOfWords); i++) {
            builder.append(nextWord()).append(" ");
        }
        return builder.toString();
    }

    public static String nextWord() {
        //1 to 10 number of characters for each random word
        return RandomStringUtils.randomAlphabetic(RANDOM.nextInt(9)+1);
    }

    public static Date nextDate() {
        int month = RANDOM.nextInt(11) + 1;
        int year = RANDOM.nextInt(30) + 1980;
        int date = RANDOM.nextInt(365);
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, date);
        return calendar.getTime();
    }

    public static int nextInt(int n) {
        return RANDOM.nextInt(n);
    }

    public static boolean nextBoolean() {
        return RANDOM.nextBoolean();
    }
}
