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

import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.processing.schema.metadata.AggregateTable;

//import mondrian.carbon.MondrianDef.Cube;

public abstract class AggregateTableSelecter {

    protected List<AggregateTable> aggregateTables;
    protected List<AggregateTable> processedAggTables = new ArrayList<AggregateTable>(15);
    protected String factName;
    AggregateTableDerivativeComposite root;

    AggregateTableSelecter(List<AggregateTable> aggregateTables) {
        this.aggregateTables = aggregateTables;

    }

    public abstract void selectTableForAggTableAggregationProcess(AggregateTable[] aggregateTable,
            Cube cube);

    public AggregateTableDerivative getAggregateTableDerivativeInstanceForAggEval() {

        return root;
    }

}
