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

#### MERGE SQL

Below sql merges a set of updates, insertions, and deletions based on a source table
into a target carbondata table. 

```
    MERGE INTO target_table_identifier
    USING source_table_identifier
    ON <merge_condition>
    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
    [ WHEN NOT MATCHED [ AND <condition> ]  THEN <not_matched_action> ]
```

#### MERGE SQL Operation Semantics
Below is the detailed description of the `merge` SQL operation.
* `table_identifier` a table name, optionally qualified with a database name
* `merge_condition` how the rows from one relation are combined with the rows of another relation. An expression with a return type of Boolean.
* `WHEN MATCHED` clauses are executed when a source row matches a target table row based on the match condition,
clauses can have at most one UPDATE and one DELETE action, These clauses have the following semantics.
    * The UPDATE action in merge only updates the specified columns of the matched target row.
    * The DELETE action will delete the matched row.
    * WHEN MATCHED clauses can have at most one UPDATE and one DELETE action. The UPDATE action in merge only updates the specified columns of the matched target row. The DELETE action will delete the matched row.
    * Each WHEN MATCHED clause can have an optional condition. If this clause condition exists, the UPDATE or DELETE action is executed for any matching source-target row pair row only when when the clause condition is true.
    * If there are multiple WHEN MATCHED clauses, then they are evaluated in order they are specified (that is, the order of the clauses matter). All WHEN MATCHED clauses, except the last one, must have conditions.
    * If both WHEN MATCHED clauses have conditions and neither of the conditions are true for a matching source-target row pair, then the matched target row is left unchanged.
    * To update all the columns of the target carbondata table with the corresponding columns of the source dataset, use UPDATE SET *. This is equivalent to UPDATE SET col1 = source.col1 [, col2 = source.col2 ...] for all the columns of the target carbondata table. Therefore, this action assumes that the source table has the same columns as those in the target table, otherwise the query will throw an analysis error.
* `matched_action` can be DELETE  | UPDATE SET *  |UPDATE SET column1 = value1 [, column2 = value2 ...]
* `WHEN NOT MATCHED` clause is executed when a source row does not match any target row based on the match condition, these clauses have the following semantics.
    * WHEN NOT MATCHED clauses can only have the INSERT action. The new row is generated based on the specified column and corresponding expressions. All the columns in the target table do not need to be specified. For unspecified target columns, NULL is inserted.
    * Each WHEN NOT MATCHED clause can have an optional condition. If the clause condition is present, a source row is inserted only if that condition is true for that row. Otherwise, the source column is ignored.
    * If there are multiple WHEN NOT MATCHED clauses, then they are evaluated in order they are specified (that is, the order of the clauses matter). All WHEN NOT  MATCHED clauses, except the last one, must have conditions.
    * To insert all the columns of the target carbondata table with the corresponding columns of the source dataset, use INSERT *. This is equivalent to INSERT (col1 [, col2 ...]) VALUES (source.col1 [, source.col2 ...]) for all the columns of the target carbondata table. Therefore, this action assumes that the source table has the same columns as those in the target table, otherwise the query will throw an error.
* `not_matched_action` can be INSERT *  | INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...])

##### Example code to implement cdc/scd scenario

Please refer example class [MergeTestCase](https://github.com/apache/carbondata/blob/master/integration/spark/src/test/scala/org/apache/carbondata/spark/testsuite/merge/MergeTestCase.scala) to understand and implement scd and cdc scenarios using api.
Please refer example class [DataMergeIntoExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/scala/org/apache/carbondata/examples/DataMergeIntoExample.scala) to understand and implement scd and cdc scenarios using sql.
