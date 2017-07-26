<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Benchmarking

 To perform and get result of Benchmarking of Parquet on Spark vs ORC on Hive vs CarbonData on Presto,
 we need to follow the steps given below in sequential order.

 * Run the class : ParquetPerformance

   Location : /examples/spark2/src/main/scala/org/apache/carbondata/examples/performance/ParquetPerformance.scala

   Task : This will capture the result of Parquet format running over Spark and load data in CarbonData store for Presto

  * Run the class : OrcPerformance

    Location : /examples/spark2/src/main/scala/org/apache/carbondata/examples/performance/OrcPerformance.scala

    Task : This will capture the result of Orc format running over Hive

  * Run the class : PrestoBenchMarking

     Location : /integration/presto/src/main/scala/org/apache/carbondata/presto/PrestoBenchMarking.scala

     Task : This will capture the result of CarbonData format running over Presto and generate comparison result.
