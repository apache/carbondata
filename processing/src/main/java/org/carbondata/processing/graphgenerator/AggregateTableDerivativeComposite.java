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

import java.util.ArrayList;
import java.util.List;

import org.carbondata.processing.schema.metadata.AggregateTable;

public class AggregateTableDerivativeComposite implements AggregateTableDerivative {

    private String evaluatorTableName;
    private AggregateTable aggregateTable;

    private List<AggregateTableDerivative> childrens = new ArrayList<AggregateTableDerivative>(20);

    public AggregateTableDerivativeComposite() {

    }

    public AggregateTableDerivativeComposite(String name, AggregateTable aggregateTable) {
        this.evaluatorTableName = name;
        this.aggregateTable = aggregateTable;
    }

    @Override
    public void add(AggregateTableDerivative aggregateTableDerivative) {
        childrens.add(aggregateTableDerivative);

    }

    @Override
    public AggregateTableDerivative get(int i) {
        return childrens.get(i);
    }

    public List<AggregateTableDerivative> getChildrens() {
        return childrens;
    }

    public int length() {
        return childrens.size();
    }

    @Override
    public void remove(AggregateTableDerivative aggregateTableDerivative) {
        childrens.remove(aggregateTableDerivative);

    }

    public String getEvaluatorTableName() {
        return evaluatorTableName;
    }

    public AggregateTable getAggregateTable() {
        return aggregateTable;
    }

}
