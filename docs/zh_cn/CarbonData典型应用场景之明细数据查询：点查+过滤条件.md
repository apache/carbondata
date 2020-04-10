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

# CarbonData典型应用场景之明细数据查询：点查+过滤条件

## 背景  

​        本文主要描述在使用CarbonData进行明细数据查询时，如何设置建表、加载、查询时的参数，以提升对应的性能。具体来说，包括在建表时如何选择合适的字典、SORT_COLUMNS、SORT_SCOPE等配置，同时我们还给出了验证场景下的环境规格、数据规格等，同时给出了配置项及对应达到的数据加载和查询性能，以便大家根据自己的业务特点和场景选择进行参数调整。

​		本文中用例数据表特点：

​			1.表记录数都比较大，范围从数千万到数百亿。

​			2.列数比较多，大约在100-600列之间。

​		使用特点：

​			1.主要是进行点查和过滤，没有汇聚计算，偶尔有关联维表的场景。

​			2.数据入库采取分批入库的方式，周期约为5分钟，按天建表。

​			3.查询时可能有不少于20的并发查询。

典型的查询的使用框架，其中第五个求sum仅为作性能对比。

| 编号 | 描述                    | 查询SQL样例                                             |
| ---- | ----------------------- | ------------------------------------------------------- |
| 1    | 点查                    | select * from table where id_a=' ' limit 1000;          |
| 2    | 模糊查询                | select * from table where id_a like '1234%' limit 1000; |
| 3    | 求记录总数              | select count(1) from table;                             |
| 4    | 求最大/最小值           | select max(id_a), min(id_a) from table;                 |
| 5    | 求sum(仅为了做性能对比) | select sum(id_a) from table;                            |

数据的特点，列主要是以int, bigint, string列构成，描述一些号码列，时间列，ID列等，无复杂数据类型。

## 测试环境

| 集群       | CPU                  | vCore | Memory | 硬盘               | 描述                                                         |
| ---------- | -------------------- | ----- | ------ | ------------------ | ------------------------------------------------------------ |
| Hadoop集群 | Gold 6132 CPU@2.60GZ | 56    | 256GB  | SATA磁盘，4T数据盘 | 2个namenode，6个datanode， 查询队列分配1/6的资源，等同于一个节点 |



## 建表 Create table 

### 建表 Create table 时字典的选择

​        建表时用户可以选择使用本地字典或者不使用字典。通常情况下使用本地字典数据加载时比较耗时，但是查询时较快; 而不使用本地字典时加载较快，查询不如使用本地字典。用户在实际使用时可以根据自身业务的诉求来选择合适的模式。关于CarbonData本地字典的具体描述可以参考官网的[文档](http://carbondata.apache.org/ddl-of-carbondata.html#create-table)。

​        为此这里做了一个验证，分别对不使用字典，使用本地字典的数据表进行加载和点查操作，分析其加载和查询性能。

建表语句结构如下：

1）不使用字典：

```sql
create table if not exists test.detail_benchmark1
('id', BIGINT, 'imsi' STRING,'msisdn' STRING, `imei` STRING, ...)
TBLPROPERTIES ( 'LOCAL_DICTIONARY_ENABLE'='false', 'table_blocksize'='256')
```

2) 使用本地字典：

```sql
create table if not exists test.detail_loacl_dict
('id', BIGINT, 'imsi' STRING,'msisdn' STRING, `imei` STRING, ...) 
TBLPROPERTIES ( 'LOCAL_DICTIONARY_INCLUDE'='IMSI,MSISDN,IMEI','table_blocksize'='256')
```

使用16亿数据量样本作为表数据，分168个批次分别加载到如上表中，其加载性能如下：

| 表                     | 数据量     | 加载耗时 秒 |
| ---------------------- | ---------- | ----------- |
| test.detail_benchmark1 | 1613149548 | 1109        |
| test.detail_loacl_dict | 1613149548 | 1876        |

下面列表记录了每一个批次的加载耗时

| 加载批次 | 耗时 秒  detail_benchmark | 耗时 秒 detail_loacl_dict |
| -------- | ------------------------- | ------------------------- |
| 1        | 6.584                     | 19.31                     |
| 2        | 10.255                    | 21.406                    |
| 3        | 5.079                     | 21.619                    |
| 4        | 5.004                     | 21.538                    |
| 5        | 10.877                    | 18.191                    |
| 6        | 7.868                     | 17.457                    |
| 7        | 5.295                     | 20.919                    |
| 8        | 4.43                      | 17.048                    |
| 9        | 5.373                     | 18.584                    |
| 10       | 6.074                     | 17.668                    |
| 11       | 5.889                     | 19.553                    |
| 12       | 4.8                       | 14.819                    |
| 13       | 4.972                     | 12                        |
| 14       | 5.144                     | 11.909                    |
| 15       | 5.403                     | 18.428                    |
| 16       | 5.314                     | 19.536                    |
| 17       | 4.519                     | 22.373                    |
| 18       | 9.327                     | 24.507                    |
| 19       | 5.277                     | 7.219                     |
| 20       | 10.882                    | 6.552                     |
| 21       | 4.74                      | 11.911                    |
| 22       | 5.38                      | 11.908                    |
| 23       | 9.046                     | 7.076                     |
| 24       | 4.399                     | 6.391                     |
| 25       | 5.079                     | 7.697                     |
| 26       | 4.177                     | 6.114                     |
| 27       | 4.441                     | 6.349                     |
| 28       | 4.951                     | 7.034                     |
| 29       | 5.09                      | 13.404                    |
| 30       | 4.286                     | 7.455                     |
| 31       | 7.821                     | 13.776                    |
| 32       | 4.623                     | 7.025                     |
| 33       | 9.304                     | 7.273                     |
| 34       | 4.806                     | 13.23                     |
| 35       | 5.469                     | 6.562                     |
| 36       | 10.045                    | 6.581                     |
| 37       | 4.619                     | 11.716                    |
| 38       | 5.807                     | 5.939                     |
| 39       | 9.101                     | 6.531                     |
| 40       | 4.968                     | 6.675                     |
| 41       | 4.654                     | 6.389                     |
| 42       | 4.763                     | 6.774                     |
| 43       | 4.155                     | 6.203                     |
| 44       | 4.672                     | 6.288                     |
| 45       | 5.052                     | 12.845                    |
| 46       | 4.812                     | 6.185                     |
| 47       | 7.941                     | 6.653                     |
| 48       | 5.401                     | 5.775                     |
| 49       | 4.684                     | 5.776                     |
| 50       | 4.698                     | 10.646                    |
| 51       | 4.604                     | 6.359                     |
| 52       | 8.843                     | 5.899                     |
| 53       | 4.785                     | 11.284                    |
| 54       | 4.969                     | 7.116                     |
| 55       | 7.634                     | 6.291                     |
| 56       | 4.424                     | 6.534                     |
| 57       | 5.086                     | 11.372                    |
| 58       | 4.461                     | 5.613                     |
| 59       | 7.809                     | 19.169                    |
| 60       | 4.852                     | 7.34                      |
| 61       | 8.486                     | 7.182                     |
| 62       | 4.412                     | 10.598                    |
| 63       | 4.168                     | 17.974                    |
| 64       | 4.287                     | 6.998                     |
| 65       | 8.867                     | 6.526                     |
| 66       | 4.37                      | 6.457                     |
| 67       | 4.23                      | 12.335                    |
| 68       | 5.28                      | 13.638                    |
| 69       | 7.945                     | 7.597                     |
| 70       | 4.647                     | 41.695                    |
| 71       | 4.429                     | 22.057                    |
| 72       | 8.659                     | 11.863                    |
| 73       | 10.062                    | 17.561                    |
| 74       | 5.514                     | 8.366                     |
| 75       | 7.608                     | 14.011                    |
| 76       | 4.586                     | 7.205                     |
| 77       | 7.692                     | 6.602                     |
| 78       | 5.319                     | 6.103                     |
| 79       | 4.884                     | 18.043                    |
| 80       | 4.13                      | 6.89                      |
| 81       | 8.511                     | 11.561                    |
| 82       | 4.983                     | 7.434                     |
| 83       | 8.486                     | 5.779                     |
| 84       | 5.158                     | 6.033                     |
| 85       | 4.224                     | 6.359                     |
| 86       | 4.444                     | 6.736                     |
| 87       | 4.546                     | 6.397                     |
| 88       | 4.799                     | 6.904                     |
| 89       | 3.967                     | 7.01                      |
| 90       | 4.437                     | 5.612                     |
| 91       | 4.541                     | 10.904                    |
| 92       | 4.649                     | 6.64                      |
| 93       | 4.665                     | 6.508                     |
| 94       | 62.925                    | 17.063                    |
| 95       | 4.335                     | 7.104                     |
| 96       | 7.366                     | 7.699                     |
| 97       | 5.028                     | 6.881                     |
| 98       | 5.097                     | 5.803                     |
| 99       | 4.345                     | 6.36                      |
| 100      | 4.251                     | 9.569                     |
| 101      | 4.671                     | 6.121                     |
| 102      | 7.432                     | 5.491                     |
| 103      | 4.508                     | 6.412                     |
| 104      | 4.866                     | 6.104                     |
| 105      | 4.58                      | 12.243                    |
| 106      | 4.94                      | 6.362                     |
| 107      | 7.842                     | 5.697                     |
| 108      | 4.332                     | 11.934                    |
| 109      | 4.646                     | 29.469                    |
| 110      | 4.534                     | 14.249                    |
| 111      | 9.206                     | 13.585                    |
| 112      | 4.802                     | 10.479                    |
| 113      | 4.119                     | 8.238                     |
| 114      | 4.139                     | 9.334                     |
| 115      | 4.588                     | 8.438                     |
| 116      | 4.537                     | 8.621                     |
| 117      | 4.688                     | 7.742                     |
| 118      | 4.055                     | 8.27                      |
| 119      | 4.703                     | 6.958                     |
| 120      | 4.51                      | 23.409                    |
| 121      | 4.178                     | 9.167                     |
| 122      | 9.22                      | 6.772                     |
| 123      | 4.359                     | 12.718                    |
| 124      | 4.926                     | 7.611                     |
| 125      | 4.487                     | 14.572                    |
| 126      | 4.571                     | 13.044                    |
| 127      | 8.108                     | 8.975                     |
| 128      | 4.472                     | 18.712                    |
| 129      | 4.66                      | 45.553                    |
| 130      | 7.791                     | 12.255                    |
| 131      | 8.569                     | 13.897                    |
| 132      | 5.671                     | 12.073                    |
| 133      | 9.43                      | 21.305                    |
| 134      | 4.015                     | 6.391                     |
| 135      | 8.961                     | 47.173                    |
| 136      | 5.955                     | 9.856                     |
| 137      | 7.095                     | 6.996                     |
| 138      | 4.641                     | 11.802                    |
| 139      | 5.156                     | 6.072                     |
| 140      | 7.757                     | 6.365                     |
| 141      | 4.71                      | 10.944                    |
| 142      | 4.346                     | 7.061                     |
| 143      | 8.663                     | 6.19                      |
| 144      | 4.641                     | 13.49                     |
| 145      | 4.864                     | 6.598                     |
| 146      | 7.932                     | 7.13                      |
| 147      | 4.56                      | 11.359                    |
| 148      | 3.941                     | 6.113                     |
| 149      | 7.729                     | 10.987                    |
| 150      | 4.7                       | 7.347                     |
| 151      | 8.089                     | 7.309                     |
| 152      | 4.016                     | 12.875                    |
| 153      | 4.468                     | 7.873                     |
| 154      | 9.136                     | 6.559                     |
| 155      | 5.69                      | 16.854                    |
| 156      | 4.253                     | 6.499                     |
| 157      | 8.223                     | 5.983                     |
| 158      | 4.733                     | 16.357                    |
| 159      | 4.347                     | 15.032                    |
| 160      | 4.276                     | 13.735                    |
| 161      | 17.02                     | 13.094                    |
| 162      | 4.765                     | 22.683                    |
| 163      | 5.941                     | 12.033                    |
| 164      | 66.877                    | 11.795                    |
| 165      | 15.101                    | 6.563                     |
| 166      | 8.748                     | 9.21                      |
| 167      | 5.412                     | 10.025                    |
| 168      | 11.692                    | 8.565                     |
| 总计     | 1109.742                  | 1876.579                  |

​        从数据中可以看出，使用本地字典在数据加载时将花费更多的时间，不使用字典时耗时明显少，从168个批次的统计数据看使用本地字典将比不使用本地字典多消耗70%的加载时长，但是在查询性能上又有提升，详细见后续介绍。

查询性能的对比

​        这里使用了一组点查的SQL对使用本地字典、全局字典和不使用字典的数据表进行了查询统计，记录一些统计结论。

点查询SQL分别使用

```sql
select * from test.detail_benchmark where MSISDN="13900000000" limit 2000;
select * from test.detail_loacl_dict where MSISDN="13900000000" limit 2000;

```

模糊查询SQL分别使用

```sql
select * from test.detail_benchmark where MSISDN like "1361%" limit 2000;
select * from test.detail_loacl_dict where MSISDN like "1391%" limit 2000;
```



记录查询耗时情况如下：

| 查询用例           | 耗时 秒 detail_benchmark | 耗时 秒 detail_loacl_dict |
| ------------------ | ------------------------ | ------------------------- |
| 点查询五次平均值   | 18.5                     | 14.543                    |
| 模糊查询五次平均值 | 10.832                   | 4.118                     |

​        从结果可以看出，使用本地字典查询较不使用本地字典点查的时候性能有较大的提升。使用本地字典点查询性能提升在30%左右，模糊查询性能提升超过1倍。

​        用户在选择字典的使用上时，可以根据自身对数据加载的要求和查询要求来选择字典，本案例由于对数据入库的时效性有较高要求，为了避免数据加载的延迟，因此在建表的时候选择无字典的方案，牺牲了一些查询性能。



### 建表 Create table 时SORT_COLUMNS和SORT_SCOPE的选择

​        关于SORT_COLUMNS的配置可以参考CarbonData[官网](http://carbondata.apache.org/ddl-of-carbondata.html#create-table)的说明, 当用户不指定SORT_COLUMNS时，默认将不建立SORT_COLUMNS，当前的SORT_COLUMNS只支持：string, date, timestamp, short, int, long, byte, boolean类型。SORT_COLUMNS 在用户指定的列上建立MKD索引，将有助于查询性能的提升，但是将稍微影响加载性能，因此只需要给需要的列上设置SORT_COLUMNS即可，无需给所有列都设置SORT_COLUMNS。

​         [SORT_SCOPE](http://carbondata.apache.org/ddl-of-carbondata.html#create-table)参数用户在指定了SORT_COLUMNS后，数据加载时排序的设置，当前支持的设置有：

NO_SORT:  对加载的数据不排序

LOCAL_SORT: 加载的数据在本地排序 

GLOBAL_SORT:  加载的数据全局排序，它提升了高并发下点查的效率，对加载的性能的影响较大。

在建表时，对使用SORT_COLUMNS时“NO_SORT”，“LOCAL_SORT”， “GLOBAL_SORT”的性能进行分析。

建表语句：

```基准表```

```sql
create table if not exists test.detail_benchmark1
('id', BIGINT, 'imsi' STRING,'msisdn' STRING, `imei` STRING, ...)
TBLPROPERTIES ( 'LOCAL_DICTIONARY_ENABLE'='false', 'table_blocksize'='256')
```

```全局排序```

```sql
create table if not exists test.detail_global_sort
('id', BIGINT, 'imsi' STRING,'msisdn' STRING, `imei` STRING, ...) 
TBLPROPERTIES ( 'LOCAL_DICTIONARY_ENABLE'='false', 'table_blocksize'='256', 'SORT_COLUMNS'='msisdn,req_time_sec,req_succed_flag', 'SORT_SCOPE'='GLOBAL_SORT')
```

```本地排序```

```sql
create table if not exists test.detail_local_sort
('id', BIGINT, 'imsi' STRING,'msisdn' STRING, `imei` STRING, ...) 
TBLPROPERTIES ( 'LOCAL_DICTIONARY_ENABLE'='false', 'table_blocksize'='256', 'SORT_COLUMNS'='msisdn,req_time_sec,req_succed_flag', 'SORT_SCOPE'='LOCAL_SORT')
```

```不排序```

```sql
create table if not exists test.detail_no_sort
('id', BIGINT, 'imsi' STRING,'msisdn' STRING, `imei` STRING, ...) 
TBLPROPERTIES ( 'LOCAL_DICTIONARY_ENABLE'='false', 'table_blocksize'='256', 'SORT_COLUMNS'='msisdn,req_time_sec,req_succed_flag', 'SORT_SCOPE'='NO_SORT')
```

验证其加载性能如下：

| 表                      | 数据量     | 加载耗时 秒 |
| ----------------------- | ---------- | ----------- |
| test.detail_benchmark   | 1613149548 | 1109        |
| test.detail_global_sort | 1613149548 | 6674        |
| test.detail_local_sort  | 1613149548 | 2473        |
| test.detail_no_sort     | 1613149548 | 1576        |

​        通过验证的结果可以看出，在SORT_COLUMNS配置后，使用GLOBAL_SORT加载耗时最高，约为local_sort的2.7倍，约为no_sort的4.2倍；使用NO_SORT加载耗时最低，LOCAL_SORT的耗时略高于NO_SORT高约50%左右。实际中采取哪种策略还要考虑对查询的诉求。

下面给出了基于上述几种场景下的点查和模糊查询的性能对比。

查询时使用的ＳＱＬ：

```sql
select * from test.detail_global_sort where MSISDN="13900000000" limit 2000;
select * from test.detail_global_sort where MSISDN like "1391%" limit 2000;

select * from test.detail_locall_sort where MSISDN="13900000000" limit 2000;
select * from test.detail_locall_sort where MSISDN like "1391%" limit 2000;

select * from test.detail_no_sort where MSISDN="13900000000" limit 2000;
select * from test.detail_no_sort where MSISDN like "1391%" limit 2000;
```

验证后性能结果如下：

| 查询五次平均值 | detail_benchmark | detail_global_sort | detail_local_sort | detail_no_sort |
| -------------- | ---------------- | ------------------ | ----------------- | -------------- |
| 点查询         | 18.5             | 1.357              | 2.457             | 5.847          |
| 模糊查询       | 10.832           | 1.785              | 1.835             | 2.164          |

​        通过查询结果可以看出，点查询和模糊查询，global_sort最快，local_sort次之，no_sort最差。点查时no_sort耗时约为global_sort的4.2倍，约为local_sort的2.3倍；global_sort比local_sort快80%左右。模糊查询时global_sort与local_sort差别不大，但是与no_sort有20%以上的提升。

​        综合考虑global_sort虽然查询最快，但是加载耗时也最高，相比local_sort在加载耗时与no_sort差别在50%左右，点查询性能提升在2倍以上，本案例最终使用local_sort作为排序方式。

​        在业务具体选择时需根据自身的诉求，结合加载和查询性能诉求综合选择采用哪种排序方式。

### 总结：

　　用户在选择字典和设置SORT_COLUMNS、SORT_SCOPE的时候需要结合自身的加载和查询诉求，综合考虑使用哪种方案。

　　本例中，明细数据查询场景对数据加载入库有较强的时效性要求，数据是一批次一批次的到来，一个批次一个批次进行加载，因此加载不可以延迟，否则将导致数据积压无法入库的问题。但是对查询也有一定的性能要求，希望用户的点查要尽可能的快。因此在综合考虑和分析后采用不建立字典，设置SORT_COLUMNS为最长查询的三个字段，SORT_SCOPE设置为LOCAL_SORT，这样的组合性能是能满足本例业务诉求。

```sql
create table IF NOT EXISTS example_table
(
`SID`        BIGINT,
`IMSI`       STRING,
`MSISDN`     STRING,
`IMEI`       STRING,
`MS_IP`      STRING,
`TMSI`       STRING,
`TRANS_TYPE`      INT,
`REQ_TIME_SEC`    BIGINT,
`REQ_SUCCED_FLAG` INT
.....
) STORED AS carbondata 
TBLPROPERTIES ( 'LOCAL_DICTIONARY_ENABLE'='false','SORT_COLUMNS'='msisdn,req_time_sec,req_succed_flag', 'SORT_SCOPE'='LOCAL_SORT' )
```





## 加载 Load data

加载样例：

```sql
LOAD DATA INPATH 'hdfs://hacluster/somepath'
INTO TABLE example_table 
OPTIONS('DELIMITER'='|', 'QUOTECHAR'='|', 'BAD_RECORDS_ACTION'='FORCE', 
'BAD_RECORDS_LOGGER_ENABLE'='false', 
'FILEHEADER'='SID,IMSI,MSISDN,IMEI,MS_IP,TMSI,TRANS_TYPE,
REQ_TIME_SEC,REQ_SUCCED_FLAG', 
'MULTILINE'='true', 'ESCAPECHAR'='\')
```

​        数据入库的时候是采用分批入库，每一个批次对应一个文件夹，加载时没有什么特殊的，主要是一些与数据相关的参数的配置。

这里面有一些参数起使用和含义如下：

DELIMITER：加载命令中可以指定数据的分隔符，本案例中是以“|”为分割，使用中可以根据数据中的具体格式来指定。

QUOTECHAR：加载命令中可以指定数据的引用字符，引用字符内的分割符将被忽略。

BAD_RECORDS_ACTION：对于坏记录，BAD_RECORDS_ACTION 属性主要有四种操作类型：FORCE, REDIRECT, IGNORE 和 FAIL。这里使用了 FORCE选项。对于该参数的其他几种选项在中文文档中有如下说明：

​        a) FAIL 是其默认的值。如果使用 FAIL 选项，则如果发现任何坏记录，数据加载将会失败。

​        b) 如果使用了 REDIRECT 选项, CarbonData 会将所有坏记录添加到单独的 CSV 文件中。 但是，在后续的数据加载我们不能使用这个文件，因为其内容可能与原始记录不完全匹配。建议你事先对原始源数据进行清理，以进行下一步的数据处理。此选项主要用于提醒你哪些记录是坏记录.

​        c) 如果使用了 FORCE 选项，那么会在加载数据之前将坏记录存储为 NULL，然后再存储

​        d) 如果使用了 IGNORE 选项，那么坏记录不会被加载，也不会写到单独的 CSV 文件中。

​        e) 在加载的数据中，如果所有记录都是坏记录，则 BAD_RECORDS_ACTION 选项将会无效，加载操作会失败。

BAD_RECORDS_LOGGER_ENABLE:  这里配置false是为了提交加载性能，遇到坏记录的时候不记录。

FILEHEADER：是对应的文件的文件头，也就是列名，实际上在加载csv文件的时候,如果源文件不存在头信息，可以在加载命令中增加该选项，来提供文件头。当加载不带头文件的csv文件，并且文件头和表的模式一致时，可以在加载时选择 'HEADER'='false'，这样就可以不指定FILEHEADER了。

MULTILINE： 设置为true，表示CSV 引号中带有换行符。

ESCAPECHAR：如果用户希望严格验证 CSV 文件中的转义字符，可以提供转义字符。

除此以外还有一些控制参数，详细可以参考：http://carbondata.iteblog.com/data-management-on-carbondata.html 中的描述。

下面给出一些加载队列的主要配置参数以供参考，更详细的信息可以参考CarbonData的官方文档：

http://carbondata.apache.org/configuration-parameters.html

| 参数名                                  | 设置数值 | 参数描述                                                     |
| --------------------------------------- | -------- | ------------------------------------------------------------ |
| enable.unsafe.sort                      | true     | 指定在数据加载期间是否使用unsafe api，提升内存操作的性能。   |
| carbon.use.local.dir                    | true     | 在数据加载期间，CarbonData在将文件复制到HDFS之前将文件写入本地临时目录。此配置用于指定CarbonData是否可以本地写入容器的tmp目录或YARN应用程序目录。 |
| carbon.number.of.cores.while.loading    | 15       | 该值默认为2，增加加载时的核数有助于提升数据加载的效率。      |
| carbon.detail.batch.size                | 100      | 存储记录的缓冲区大小，这里是从bolck扫描返回的数据量。该值的设置需要根据查询是需要返回的数据量的大小，当查询时返回数据限制是1000条，参数配置为3000条就有数据浪费了。 |
| carbon.sort.file.buffer.size            | 50       | 排序期间使用的文件读取缓冲区大小（以MB为单位）：MIN=1:MAX=100。 |
| max.query.execution.time                | 60       | 查询执行的最大时间，该值的单位是分钟。                       |
| carbon.unsafe.working.memory.in.mb      | 16000    | 该数值是CarbonData支持将数据存储在堆外内存中，以便在数据加载和查询期间执行某些操作。这有助于避免Java GC，从而提高整体性能。建议的最小值为512MB。低于此值的任何值都将重置为默认值512MB。官网有公式介绍了如何计算对外内存的大小：（carbon.number.of.cores.while.Loading）*（要并行加载的表数）*（off heap.sort.chunk.size.in mb+carbon.blockletgroup.size.in.mb+carbon.blockletgroup.size.in.mb/3.5） |
| carbon.compaction.level.threshold       | 6,0      | 配置此参数用于将小文件合并，提升查询性能。6表示第一阶段压缩，每6个文件将触发一次level1的压缩，这里并没有使用level2压缩，为了节省入库时间。配置时由于有大表，有小表，因此对于合并是单表控制的，未设置全局开关。详见社区配置[说明](http://carbondata.apache.org/ddl-of-carbondata.html#create-table)中Table Compaction Configuration章节。 |
| carbon.number.of.cores.while.compacting | 15       | 在压缩过程中写数据使用到的核数，这里默认数值是2。            |
| carbon.sort.storage.inmemory.size.inmb  | 1024     | CarbonData在数据加载期间将每个carbon.sort.size记录数写入中间临时文件，以确保内存占用在限制范围内。启用enable.unsafe.sort配置时，将使用内存中占用的大小（本参数）来确定何时将数据页刷新到中间临时文件，而不是使用基于行数的carbon.sort.size。此配置确定用于在内存中存储数据页的内存。注意：配置一个更高的值可以确保在内存中保留更多的数据，从而由于IO减少或没有IO而提高数据加载性能。根据集群节点中的内存可用性，相应地配置这些值。 |

​        在进行数据加载时，根据数据量可以划分多个加载队列，分配不同的资源，加载时根据入库的数据量选择不同的加载队列进行加载，这样可以有效避免大数据量表的加载阻塞加载队列，导致小数据量表入库延迟，这部分数据具体的业务规划和实现，就不在这里过多介绍了。

​         数据加载耗时情况根据选择的配置参数不同而不同，详细数据参见建表章节的性能对比。

## 查询 Select 
查询主要的测试用例在背景处已经介绍。

一些主要的查询配置：

| 参数名                          | 参数数值 | 参数描述                                                     |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| spark.dynamicAllocation.enabled | false    | 是否开启动态资源配置参数，这里选择关闭，executor选择常驻方式启动，在查询时如果开启动态分配可能消耗掉额外的时间。 |
| spark.sql.shuffle.partitions    | 78       | 该数值配置为spark_executor_instances*spark_executor_cores    |
| spark.driver.cores              | 3        | 在集群模式下管理资源时，用于driver程序的CPU内核数量，这里设置为3。 |
| spark.driver.maxResultSize      | 4G       | 存储分区中结果集大小。                                       |
| spark.locality.wait             | 500      | task在执行前都会获取数据的分区信息进行分配，总是会优先将其分配到它要计算的数据所在节点，尽可能的减少网络传输，一般在进行数据本地化分配的时候会选择PROCESS_LOCAL，一旦分配超时失败将降级分配，这里将超时时间改为0.5秒。 |
| spark.speculation               | true     | 启动spark推测执行，这里目的是为了解决集群中拖尾任务耗时较长的场景。 |
| spark.speculation.quantile      | 0.75     | 同一个stage里面所有task完成的百分比。                        |
| spark.speculation.multiplier    | 5        | 这个参数是一个时间比例，让task完成了spark.speculation.quantile定义的比例的时候，取完成了的task的执行时间的中位数，再乘以这个时间比例，当未完成的时间超过这个乘积时，启动推测执行。 |
| spark.blacklist.enabled         | false    | 关闭黑名单，避免可用executor数量不足。                       |

查询性能统计

单并发场景：

| 测试用例描述               | 数据量 | 并发数 | 耗时（秒） |
| :------------------------- | ------ | ------ | ---------- |
| 点查非SORT_COLUMNS字段     | 27亿   | 1      | 2.790      |
| 点查SORT_COLUMNS字段       | 27亿   | 1      | 3.047      |
| 模糊查询非SORT_COLUMNS字段 | 27亿   | 1      | 1.517      |
| 模糊查询SORT_COLUMNS字段   | 27亿   | 1      | 1.673      |
| 求记录总数                 | 27亿   | 1      | 2.017      |
| 求最大最小值               | 27亿   | 1      | 2.567      |
| 求sum                      | 27亿   | 1      | 3.413      |

多并发场景：

| 测试用例描述               | 数据量 | 并发数 | 耗时（秒） |
| -------------------------- | ------ | ------ | ---------- |
| 点查非SORT_COLUMNS字段     | 27亿   | 10     | 5.886      |
|                            | 27亿   | 20     | 8.657      |
| 点查SORT_COLUMNS字段       | 27亿   | 10     | 5.737      |
|                            | 27亿   | 20     | 8.564      |
| 模糊查询非SORT_COLUMNS字段 | 27亿   | 10     | 2.906      |
|                            | 27亿   | 20     | 3.957      |
| 模糊查询SORT_COLUMNS字段   | 27亿   | 10     | 2.794      |
|                            | 27亿   | 20     | 3.988      |
| 求记录总数                 | 27亿   | 10     | 12.556     |
|                            | 27亿   | 20     | 22.660     |
| 求最大最小值               | 27亿   | 10     | 18.201     |
|                            | 27亿   | 20     | 33.917     |
| 求sum                      | 27亿   | 10     | 19.811     |
|                            | 27亿   | 20     | 36.595     |

​        通过测试结果数据可以看出，在27亿左右数据量场景下，单并发点查、模糊查询、记录数、最大、最小值、求和的耗时在4秒以内，模糊查询耗时短于精确查询；在多并发场景下，耗时较单并发有增加，在10并发时点查和模糊查询耗时增加一倍，求记录数，最大，最小值，求和的耗时增加比较明显，达到5-8倍。在20并发情况下，点查、模糊查询、求记录数、最大、最小、求和的耗时又在10并发的基础上增加接近1倍。但是即使在20并发情况下点查和模糊查询的耗时依然在10秒以内，还可用通过增加查询资源来提高并发查询的效率。

​        本案例给出了一个使用CarbonData进行明细数据查询的场景的建表、加载、查询的整个流程，也对建表时字典和SORT_COLUMNS的配置及SORT_SCOPE的配置方式给出了介绍和对比。通过本文可以指导用户使用CarbonData进行明细数据的查询场景的方案设置和参数选择。从最终的结果看，CarbonData在明细数据查询的场景下也具有不错的性能表现。

