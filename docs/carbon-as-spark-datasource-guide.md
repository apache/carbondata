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

# CarbonData as Spark's Datasource

The CarbonData fileformat is now integrated as Spark datasource for read and write operation without using CarbonSession. This is useful for users who wants to use carbondata as spark's data source. 

**Note:** You can only apply the functions/features supported by spark datasource APIs, functionalities supported would be similar to Parquet. The carbon session features are not supported. The result is displayed as byte array format when select query on binary column in spark-sql.

# Create Table with DDL

Now you can create Carbon table using Spark's datasource DDL syntax.

```
 CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db_name.]table_name
     [(col_name1 col_type1 [COMMENT col_comment1], ...)]
     USING CARBON
     [OPTIONS (key1=val1, key2=val2, ...)]
     [PARTITIONED BY (col_name1, col_name2, ...)]
     [CLUSTERED BY (col_name3, col_name4, ...) INTO num_buckets BUCKETS]
     [LOCATION path]
     [COMMENT table_comment]
     [TBLPROPERTIES (key1=val1, key2=val2, ...)]
     [AS select_statement]
``` 

## Supported OPTIONS

| Property | Default Value | Description |
|-----------|--------------|------------|
| table_blocksize | 1024 | Size of blocks to write onto hdfs. For more details, see [Table Block Size Configuration](./ddl-of-carbondata.md#table-block-size-configuration). |
| table_blocklet_size | 64 | Size of blocklet to write. |
| table_page_size_inmb | 0 | Size of each page in carbon table, if page size crosses this value before 32000 rows, page will be cut to that many rows. Helps in keep page size to fit cache size |
| local_dictionary_threshold | 10000 | Cardinality upto which the local dictionary can be generated. For more details, see [Local Dictionary Configuration](./ddl-of-carbondata.md#local-dictionary-configuration). |
| local_dictionary_enable | false | Enable local dictionary generation. For more details, see [Local Dictionary Configuration](./ddl-of-carbondata.md#local-dictionary-configuration). |
| sort_columns | all dimensions are sorted | Columns to include in sort and its order of sort. For more details, see [Sort Columns Configuration](./ddl-of-carbondata.md#sort-columns-configuration). |
| sort_scope | local_sort | Sort scope of the load.Options include no sort, local sort, batch sort, and global sort. For more details, see [Sort Scope Configuration](./ddl-of-carbondata.md#sort-scope-configuration). |
| long_string_columns | null | Comma separated string/char/varchar columns which are more than 32k length. For more details, see [String longer than 32000 characters](./ddl-of-carbondata.md#string-longer-than-32000-characters). |

 **NOTE:**  please set long_string_columns for varchar column.
## Example 

```
 CREATE TABLE CARBON_TABLE (NAME STRING) USING CARBON OPTIONS('table_block_size'='256')
```

# Using DataFrame

Carbon format can be used in dataframe also. Following are the ways to use carbon format in dataframe.

Write carbon using dataframe 
```
df.write.format("carbon").save(path)
```

Read carbon using dataframe
```
val df = spark.read.format("carbon").load(path)
```

## Example

```
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._
val df = spark.sparkContext.parallelize(1 to 10 * 10 * 1000)
     .map(x => (r.nextInt(100000), "name" + x % 8, "city" + x % 50, BigDecimal.apply(x % 60)))
      .toDF("ID", "name", "city", "age")
      
// Write to carbon format      
df.write.format("carbon").save("/user/person_table")

// Read carbon using dataframe
val dfread = spark.read.format("carbon").load("/user/person_table")
dfread.show()
```

Reference : [list of carbon properties](./configuration-parameters.md)

