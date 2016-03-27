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

package org.carbondata.processing.graphgenerator;

import java.util.Arrays;
import java.util.List;

import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.processing.schema.metadata.AggregateTable;

//import mondrian.olap.MondrianDef.Cube;

public class AutoAggregateTableSelecter extends AggregateTableSelecter {
    private boolean isMatching;

    public AutoAggregateTableSelecter(List<AggregateTable> aggregateTables) {
        super(aggregateTables);

    }

    public void selectTableForAggTableAggregationProcess(AggregateTable[] aggregateTablesArray,
            Cube cube) {

        AggregateTableDerivativeComposite aggregateTableDerivative = null;
        for (int i = 0; i < aggregateTables.size(); i++) {

            if (i == 0) {
                factName = cube.fact.getAlias();
                root = new AggregateTableDerivativeComposite();
                aggregateTableDerivative =
                        new AggregateTableDerivativeComposite(factName, aggregateTables.get(i));
                root.add(aggregateTableDerivative);
            } else {

                if (aggregateTableDerivative != null) {

                    addAggTableRelation(root, aggregateTables.get(i));
                    if (!isMatching) {
                        AggregateTableDerivative aggregateTableDerivativetemp =
                                new AggregateTableDerivativeComposite(factName,
                                        aggregateTables.get(i));
                        root.add(aggregateTableDerivativetemp);
                    }
                }

            }

        }

    }

    private void addAggTableRelation(AggregateTableDerivative aggregateTableDerivative,
            AggregateTable aggregateTable) {

        List<AggregateTableDerivative> listOfAggregateTableDerivativeChildren =
                aggregateTableDerivative.getChildrens();

        AggregateTable aggregateTableOfDerivativecomp = null;

        if (listOfAggregateTableDerivativeChildren.isEmpty()) {
            aggregateTableOfDerivativecomp =
                    ((AggregateTableDerivativeComposite) aggregateTableDerivative)
                            .getAggregateTable();
            //			}
            isMatching =
                    isMatchingTable(aggregateTable, aggregateTableOfDerivativecomp, isMatching);
            if (isMatching) {
                AggregateTableDerivative derivedTable = new AggregateTableDerivativeComposite(
                        aggregateTableOfDerivativecomp.getAggregateTableName(), aggregateTable);
                if (!processedAggTables.contains(aggregateTable)) {
                    aggregateTableDerivative.add(derivedTable);
                    processedAggTables.add(aggregateTable);
                }

            }
        } else {
            for (int j = listOfAggregateTableDerivativeChildren.size() - 1; j >= 0; j--) {

                AggregateTableDerivative aggTableDerivativeChild =
                        listOfAggregateTableDerivativeChildren.get(j);

                addAggTableRelation(aggTableDerivativeChild, aggregateTable);

                if (!isMatching) {
                    addNode(aggTableDerivativeChild, aggregateTable);
                }
            }
        }

    }

    private void addNode(AggregateTableDerivative aggTableDerivativeChild,
            AggregateTable aggregateTable) {
        AggregateTable aggregateTableOfDerivativecomp = null;

        aggregateTableOfDerivativecomp =
                ((AggregateTableDerivativeComposite) aggTableDerivativeChild).getAggregateTable();
        isMatching = isMatchingTable(aggregateTable, aggregateTableOfDerivativecomp, isMatching);
        if (isMatching) {
            AggregateTableDerivative derivedTable = new AggregateTableDerivativeComposite(
                    aggregateTableOfDerivativecomp.getAggregateTableName(), aggregateTable);
            if (!processedAggTables.contains(aggregateTable)) {
                aggTableDerivativeChild.add(derivedTable);
                processedAggTables.add(aggregateTable);
            }
        }

    }

    private boolean isMatchingTable(AggregateTable aggregateTable,
            AggregateTable aggregateTableOfDerivativecomp, boolean isMatching) {
        List<String> levelsList =
                Arrays.asList(aggregateTableOfDerivativecomp.getActualAggLevels());
        for (String level : aggregateTable.getActualAggLevels()) {
            isMatching = levelsList.contains(level);
            if (!isMatching) {
                return isMatching;
            }
        }
        return isMatching;
    }

}
