<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# Upsert into a Carbon DataSet using Merge 

## SCD and CDC Scenarios
Change Data Capture (CDC), is to apply all data changes generated from an external data set 
into a target dataset. In other words, a set of changes (update/delete/insert) applied to an external 
table needs to be applied to a target table.

Slowly Changing Dimensions (SCD), are the dimensions in which the data changes slowly, rather 
than changing regularly on a time basis.

SCD and CDC data changes can be merged to a carbon dataset online using the data frame level `MERGE` API.

#### MERGE API

Below API merges the datasets online and applies the actions as per the conditions. 

```
  targetDS.merge(sourceDS, <condition>)
          .whenMatched(<condition>)
          .updateExpr(updateMap)
          .insertExpr(insertMap_u)
          .whenNotMatched(<condition>)
          .insertExpr(insertMap)
          .whenNotMatchedAndExistsOnlyOnTarget(<condition>)
          .delete()
          .execute()
```

#### MERGE API Operation Semantics
Below is the detailed description of the `merge` API operation.
* `merge` will merge the datasets based on a condition.
* `whenMatched` clauses are executed when a source row matches a target table row based on the match condition.
   These clauses have the following semantics.
    * `whenMatched` clauses can have at most one updateExpr and one delete action. The `updateExpr` action in merge only updates the specified columns of the matched target row. The `delete` action deletes the matched row.
    * If there are two `whenMatched` clauses, then they are evaluated in order they are specified. The first clause must have a clause condition (otherwise the second clause is never executed).
    * If both `whenMatched` clauses have conditions and neither of the conditions are true for a matching source-target row pair, then the matched target row is left unchanged.
* `whenNotMatched` clause is executed when a source row does not match any target row based on the match condition.
   * `whenNotMatched` clause can have only the `insertExpr` action. The new row is generated based on the specified column and corresponding expressions. Users do not need to specify all the columns in the target table. For unspecified target columns, NULL is inserted.
* `whenNotMatchedAndExistsOnlyOnTarget` clause is executed when row does not match source and exists only in target. This clause can have only delete action.

**NOTE:** SQL syntax for merge is not yet supported.

##### Example code to implement cdc/scd scenario

Please refer example class [MergeTestCase](https://github.com/apache/carbondata/blob/master/integration/spark/src/test/scala/org/apache/carbondata/spark/testsuite/merge/MergeTestCase.scala) to understand and implement scd and cdc scenarios.
