/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.carbondata.query.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.constants.MolapCommonConstants;

/**
 * It is used to create and parse the MOLAP query. It is just like a SQL query.
 */
public class MolapSQLQuery {

    /**
     * SELECT
     */
    private static final String SELECT = "SELECT";
    /**
     * WHERE
     */
    private static final String WHERE = "WHERE";
    /**
     * FROM
     */
    private static final String FROM = "FROM";
    /**
     * ORDER_BY
     */
    private static final String ORDER_BY = "ORDER BY";

    /**
     * order
     */
    //private Map<String, String> order = new LinkedHashMap<String, String>();
    /**
     * SPACE
     */
    private static final String SPACE = " ";
    /**
     * COMMA
     */
    private static final String COMMA = ",";
    /**
     * EQUAL
     */
    private static final String EQUAL = "=";
    /**
     * AND
     */
    private static final String AND = "AND";
    /**
     * LEFT_BRACKET
     */
    private static final String LEFT_BRACKET = "(";
    /**
     * RIGHT_BRACKET
     */
    private static final String RIGHT_BRACKET = ")";
    /**
     * DOT
     */
    private static final String DOT = ".";
    private static final String BLANK = "B!@&K";
    /**
     * SPECIAL_COMMA
     */
    private static final String SPECIAL_COMMA = "@#COMMA#@";
    /**
     * select
     */
    private List<String> select = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    /**
     * tables
     */
    private List<String> tables = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    /**
     * pred
     */
    private Map<String, List<String>> pred = new LinkedHashMap<String, List<String>>();
    /**
     * aggs
     */
    private Map<String, String> aggs = new LinkedHashMap<String, String>();
    /**
     * schemaName
     */
    private String schemaName;
    /**
     * cubeName
     */
    private String cubeName;

    /**
     * It parses the query to MOLAPSQLQuery object
     *
     * @param query
     * @return MolapSQLQuery
     */
    public static MolapSQLQuery parseQuery(String query) {
        MolapSQLQuery mSQuery = new MolapSQLQuery();

        String[] fromSplit = query.split(FROM);

        String columns = fromSplit[0].substring(SELECT.length() + 1, fromSplit[0].length());
        createListFromString(columns, mSQuery.select);
        createAggFromString(columns, mSQuery.aggs);

        String[] whereSplit = fromSplit[1].split(WHERE);
        createTablesFromString(whereSplit[0], mSQuery.tables, mSQuery);

        String[] orderSplit = whereSplit[1].split(ORDER_BY);

        String[] predSplit = orderSplit[0].split(AND);

        for (int i = 0; i < predSplit.length; i++) {
            if (predSplit[i].trim().length() > 0) {
                String[] preds = predSplit[i].split(EQUAL);
                if (preds.length > 1) {
                    List<String> vals =
                            new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    createListFromString(preds[1], vals);
                    mSQuery.pred.put(preds[0], vals);
                }
            }

        }

        //        if(orderSplit.length > 1)
        //        {
        //
        //        }

        return mSQuery;
    }

    /**
     * creates list of columns from query
     *
     * @param query
     * @param list
     */
    private static void createListFromString(String query, List<String> list) {
        String[] cols = query.split(COMMA);

        for (int i = 0; i < cols.length; i++) {
            String col = cols[i].trim();
            if (!col.contains(LEFT_BRACKET)) {
                if (cols[i].trim().equals(BLANK)) {
                    cols[i] = "";
                }
                cols[i] = cols[i].trim().replace("D$$F#|T", EQUAL);
                cols[i] = cols[i].trim().replace(SPECIAL_COMMA, COMMA);
                list.add(cols[i]);
            }
        }
    }

    /**
     * creates list of columns from query
     *
     * @param query
     * @param list
     */
    private static void createTablesFromString(String query, List<String> list,
            MolapSQLQuery mSQuery) {
        String[] cols = query.split(COMMA);

        for (int i = 0; i < cols.length; i++) {
            String col = cols[i].trim();
            String tableName = col.substring(0, col.indexOf(LEFT_BRACKET));
            list.add(tableName);
            String schemaAndCube = col.substring(col.indexOf(LEFT_BRACKET) + 1, col.length() - 1);
            String[] schemaAndCubeSlit = schemaAndCube.split("\\.");
            mSQuery.schemaName = schemaAndCubeSlit[0];
            mSQuery.cubeName = schemaAndCubeSlit[1];
        }
    }

    /**
     * creates list of aggs from query
     *
     * @param query
     * @param list
     */
    private static void createAggFromString(String query, Map<String, String> aggMap) {
        String[] cols = query.split(COMMA);

        for (int i = 0; i < cols.length; i++) {
            String col = cols[i].trim();
            if (col.contains(LEFT_BRACKET)) {
                String aggName = col.substring(0, col.indexOf(LEFT_BRACKET));
                String actualCol = col.substring(col.indexOf(LEFT_BRACKET) + 1, col.length() - 1);
                aggMap.put(actualCol, aggName);
            }
        }
    }

    /**
     * Add table to query
     *
     * @param table
     */
    public void addTable(String table) {
        tables.add(table);
    }

    /**
     * Add column to query
     *
     * @param column
     */
    public void addColumn(String column) {
        select.add(column);
    }

    /**
     * Add filter to the query
     *
     * @param column
     * @param filters
     */
    public void addFilter(String column, List<String> filters) {
        if (filters != null) {
            for (int i = 0; i < filters.size(); i++) {
                String filter = filters.get(i);
                filter = filter.replace(EQUAL, "D$$F#|T");
                filter = filter.replace(COMMA, SPECIAL_COMMA);
                if (filter.length() == 0) {
                    filter = BLANK;
                }
                filters.set(i, filter);
            }
        }
        pred.put(column, filters);
    }

    /**
     * Add Measure to the query.
     *
     * @param column
     * @param aggName
     */
    public void addAggColumn(String column, String aggName) {
        aggs.put(column, aggName);
    }

    /**
     * toString method
     */
    @Override public String toString() {
        return buildQuery();
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName() {
        return cubeName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    /**
     * @return the tables
     */
    public List<String> getTables() {
        return tables;
    }

    /**
     * @return the pred
     */
    public Map<String, List<String>> getPred() {
        return pred;
    }

    /**
     * @return the aggs
     */
    public Map<String, String> getAggs() {
        return aggs;
    }

    /**
     * @param select the select to set
     */
    public List<String> getSelects() {
        return select;
    }

    /**
     * Builds the SQL type MOLAP query
     *
     * @return String query
     */
    public String buildQuery() {
        StringBuilder builder = new StringBuilder();

        builder.append(SELECT).append(SPACE);

        iterateAndAdd(builder, select);
        iterateAggs(builder, aggs);
        builder.append(SPACE);
        builder.append(FROM);
        builder.append(SPACE);

        iterateAndAddTable(builder, tables);

        builder.append(SPACE);
        builder.append(WHERE);
        builder.append(SPACE);

        int i = 0;
        for (Entry<String, List<String>> entry : pred.entrySet()) {
            builder.append(entry.getKey()).append(EQUAL);
            iterateAndAdd(builder, entry.getValue());

            if (i < pred.size() - 1) {
                builder.append(SPACE);
                builder.append(AND);
            }

            i++;
        }

        return builder.toString();
    }

    /**
     * Add the measures to builder
     *
     * @param builder
     * @param aggs
     */
    private void iterateAggs(StringBuilder builder, Map<String, String> aggs) {
        if (aggs.size() > 0) {
            builder.append(COMMA);
        }
        int i = 0;
        for (Entry<String, String> entry : aggs.entrySet()) {
            builder.append(entry.getValue()).append(LEFT_BRACKET).append(entry.getKey())
                    .append(RIGHT_BRACKET);

            if (i < aggs.size() - 1) {
                builder.append(COMMA);
            }
            i++;
        }
    }

    /**
     * Iterate and add to builder
     *
     * @param builder
     */
    private void iterateAndAdd(StringBuilder builder, List<String> select) {
        int i = 0;
        for (String selectVal : select) {
            builder.append(selectVal);
            if (i < select.size() - 1) {
                builder.append(COMMA);
            }

            i++;
        }
    }

    /**
     * Iterate and add to builder
     *
     * @param builder
     */
    private void iterateAndAddTable(StringBuilder builder, List<String> select) {
        int i = 0;
        for (String selectVal : select) {
            builder.append(selectVal).append(LEFT_BRACKET).append(schemaName).append(DOT)
                    .append(cubeName).append(RIGHT_BRACKET);
            if (i < select.size() - 1) {
                builder.append(COMMA);
            }

            i++;
        }
    }

    //    public static void main(String[] args)
    //    {
    //        MolapSQLQuery parseQuery = parseQuery("SELECT c1,c2,c3,c4,max(c6),count(c7) FROM t1(s1.c1) WHERE c1=1,2,3 AND c8=23,25");
    //
    //        System.out.println(parseQuery);
    //
    //    }

}
