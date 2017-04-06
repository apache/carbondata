# CarbonData Benchmarking

## Pre-requisites

1. Data should exist in Hive Table in ORC Format
2. Data should exist in CarbonData Table in Carbon Format
3. Hive Metastore must be running/accessible
4. CarbonData Carbonstore must be running/accessible

### Hive
In order to create data in HIVE in ORC format follow the below queries:

```
create database benchmarking;
```
```
use benchmarking;
```
```
create table uniq_data_temp(
col1 String, col2 String, col3 String, col4 String, col5 String, col6 String, col7 String, col8 String, col9 String, col10 String, col31 int, col32 int, col33 int, col34 int, col35 int, col36 int, col37 int, col38 int, col39 int, col40 int) row format delimited fields terminated by ',' stored as textfile;
```
```
create table uniq_data(
col1 String, col2 String, col3 String, col4 String, col5 String, col6 String, col7 String, col8 String, col9 String, col10 String, col31 int, col32 int, col33 int, col34 int, col35 int, col36 int, col37 int, col38 int, col39 int, col40 int) row format delimited fields terminated by ',' stored as orc;
```
```
LOAD DATA INPATH 'hdfs://localhost:54310/data_csv/uniqdata_bench3.csv' INTO TABLE uniq_data_temp;
```
```
insert into table uniq_data select * from uniq_data_temp;
```



### CarbonData
```
create database benchmarking;
```
```
use benchmarking;
```
```
create table uniq_data(
col1 String, col2 String, col3 String, col4 String, col5 String, col6 String, col7 String, col8 String, col9 String, col10 String, col31 int, col32 int, col33 int, col34 int, col35 int, col36 int, col37 int, col38 int, col39 int, col40 int) stored by 'carbondata';
```
```
LOAD DATA INPATH 'hdfs://localhost:54310/data_csv/uniqdata_bench3.csv' into table uniq_data OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col31, col32, col33, col34, col35, col36, col37, col38, col39, col40');
```

After successful creation of Data in Hive and CarbonData, inorder to benchmark we need to set the following things:

1. In the PrestoServerRunner set the values for

   - CARBONDATA_STOREPATH: location of CarbonData metastore

2. In the PrestoClientRunner set the values for

   -  USER = "username"
   -  PASS = "password"

3. In the PrestoHiveServerRunner set the values for

   -  HIVE_METASTORE_URI : the default value for metastore has been set

4. In the PrestoHiveClientRunner set the values for

    -  USER = "username"
    -  PASS = "password"

### Benchmarking

Follow the steps given below:

1. Start the hive metastore service:
    ```
    hive --service metastore --hiveconf hive.root.logger=DEBUG,console
    ```

2. Start the HDFS (if CarbonStore is on the HDFS )

3. Execute CompareTest

4. The results for Benchmarking are stored in : examples/presto/src/main/resources/BenchmarkingResults.txt
