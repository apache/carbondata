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

# Apache CarbonData Documentation



Apache CarbonData is a new big data file format for faster interactive query using advanced columnar storage, index, compression and encoding techniques to improve computing efficiency, which helps in speeding up queries by an order of magnitude faster over PetaBytes of data.



## Getting Started

**File Format Concepts:** Start with the basics of understanding the [CarbonData file format](./file-structure-of-carbondata.md#carbondata-file-format) and its [storage structure](./file-structure-of-carbondata.md). This will help to understand other parts of the documentation, including deployment, programming and usage guides. 

**Quick Start:** [Run an example program](./quick-start-guide.md#installing-and-configuring-carbondata-to-run-locally-with-spark-shell) on your local machine or [study some examples](https://github.com/apache/carbondata/tree/master/examples/spark/src/main/scala/org/apache/carbondata/examples).

**CarbonData SQL Language Reference:** CarbonData extends the Spark SQL language and adds several [DDL](./ddl-of-carbondata.md) and [DML](./dml-of-carbondata.md) statements to support operations on it. Refer to the [Reference Manual](./language-manual.md) to understand the supported features and functions.

**Programming Guides:** You can read our guides about [Java APIs supported](./sdk-guide.md) or [C++ APIs supported](./csdk-guide.md) to learn how to integrate CarbonData with your applications.



## Integration

 - CarbonData can be integrated with popular execution engines like [Spark](./quick-start-guide.md#spark) , [Presto](./quick-start-guide.md#presto) and [Hive](./quick-start-guide.md#hive).
 - CarbonData can be integrated with popular storage engines like HDFS, Huawei Cloud(OBS) and [Alluxio](./quick-start-guide.md#alluxio).   
  Refer to the [Installation and Configuration](./quick-start-guide.md#integration) section to understand all modes of Integrating CarbonData.



## Contributing to CarbonData

The Apache CarbonData community welcomes all kinds of contributions from anyone with a passion for
faster data format.Contributing to CarbonData doesn’t just mean writing code. Helping new users on the mailing list, testing releases, and improving documentation are also welcome.Please follow the [Contributing to CarbonData guidelines](./how-to-contribute-to-apache-carbondata.md) before proposing a design or code change.



**Compiling CarbonData:** This [guide](https://github.com/apache/carbondata/tree/master/build) will help you to compile and generate the jars for test.



## External Resources

**Wiki:** You can read the [Apache CarbonData wiki](https://cwiki.apache.org/confluence/display/CARBONDATA/CarbonData+Home) page for upcoming release plan, blogs and training materials.

**Summit:** Presentations from past summits and conferences can be found [here](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=66850609).

**Blogs:** Blogs by external users can be found [here](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=67635497).

**Performance reports:** TPC-H performance reports can be found [here](https://cwiki.apache.org/confluence/display/CARBONDATA/Performance+-+TPCH+Report+of+CarbonData+%281.2+version%29+and+Parquet+on+Spark+Execution+Engine).

**Trainings:** Training records on design and code flows can be found [here](https://cwiki.apache.org/confluence/display/CARBONDATA/CarbonData+Training+Materials).

