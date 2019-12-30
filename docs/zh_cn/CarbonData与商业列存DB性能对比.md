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

## CarbonData与商业列存DB性能对比

本文描述了CarbonData与某商业列存DB的查询性能对比，通过此对比可以看出CarbonData的优势和特点。本文的测试场景是基于某聚合查询业务（分析报表），测试结果只代表该特定查询场景下的性能对比。





## 1. 测试环境

商业列存DB使用SSD硬盘，配置一台查询节点。CarbonData6个DataNode，配置STAT硬盘，但是查询队列设置1/6的资源，等同于1台商业DB服务器对比1台CarbonData服务器的查询性能。同时CarbonData使用的服务器的磁盘是STAT盘，成本比某商业列存DB服务器低。

| 集群             | CPU                  | vCore | Memory | 硬盘 | 描述                                                         |
| ---------------- | -------------------- | ----- | ------ | ---- | ------------------------------------------------------------ |
| 某商业列存DB集群 | Gold 6132 CPU@2.60GZ | 56    | 256GB  | SSD  | 3节点，一个节点作为查询节点                                  |
| Hadoop集群       | Gold 6132 CPU@2.60GZ | 56    | 256GB  | SATA | 2个namenode，6个datanode， 查询队列分配1/6的资源，等同于一个节点 |



## 2. SQL测试语句介绍

```Spark SQL的查询语句：```

```SQL
SELECT 
  COALESCE(SUM(COLUMN_A), 0) + COALESCE(SUM(COLUMN_B), 0) AS COLUMN_C , 
  COALESCE(SUM(COLUMN_A), 0) AS COLUMN_A_A ,
  COALESCE(SUM(COLUMN_B), 0) AS COLUMN_B_B , 
  COALESCE(SUM(COLUMN_D), 0) + COALESCE(SUM(COLUMN_E), 0) AS COLUMN_F , 
  COALESCE(SUM(COLUMN_D), 0) AS COLUMN_D_D , 
  COALESCE(SUM(COLUMN_E), 0) AS COLUMN_E_E ,
  (COALESCE(SUM(COLUMN_A), 0) + COALESCE(SUM(COLUMN_B), 0)) * delta AS COLUMN_F , 
  COALESCE(SUM(COLUMN_A), 0) * delta AS COLUMN_G , 
  COALESCE(SUM(COLUMN_B), 0) * delta AS COLUMN_H , 
  MT.`TEMP` AS `TEMP` 
FROM ( 
	SELECT 
		`COLUMN_1_A` AS COLUMN_A, 
		`COLUMN_1_E` AS COLUMN_E, 
		`COLUMN_1_B` AS COLUMN_B, 
		`COLUMN_1_D` AS COLUMN_D, 
		TABLE_A.`TEMP` AS `TEMP` 
	FROM TABLE_B LEFT JOIN ( 
			SELECT 
				`COLUMN_CSI` AS `TEMP2` , 
				CASE WHEN `TYPE_ID` = 2 THEN `COLUMN_CSI` END AS `TEMP` , 
				CASE WHEN `TYPE_ID` = 2 THEN `COLUMN_NAME` END AS NAME_TEMP 
			FROM DIMENSION_TABLE 
			GROUP BY 
				`COLUMN_CSI`, 
				CASE WHEN `TYPE_ID` = 2 THEN `COLUMN_CSI` END, 
				CASE WHEN `TYPE_ID` = 2 THEN `COLUMN_NAME` END
	) TABLE_A 
	ON `COLUMN_CSI` = TABLE_A.`TEMP2` 
	WHERE 
		TABLE_A.NAME_TEMP IS NOT NULL AND 
		`TIME` >= A AND `TIME` < B 
) MT 
GROUP BY MT.`TEMP` 
ORDER BY COLUMN_C DESC 
LIMIT 5000
```

其中一个SUM后面称为一个counter



## 3. CarbonData主要配置参数

```主要配置```

| CarbonData主要配置                   | 参数值 | 描述                                                         |
| ------------------------------------ | ------ | ------------------------------------------------------------ |
| carbon.inmemory.record.size          | 480000 | 查询每个表需要加载到内存的总行数。                           |
| carbon.number.of.cores               | 4      | carbon查询过程中并行扫描的线程数。                           |
| carbon.number.of.cores.while.loading | 15     | carbon数据加载过程中并行扫描的线程数。                       |
| carbon.sort.file.buffer.size         | 20     | 在合并排序(读/写)操作时存储每个临时过程文件的所使用的总缓存大小。单位为MB |
| carbon.sort.size                     | 500000 | 在数据加载操作时，每次被排序的记录数。                       |
| Spark主要配置                        |        |                                                              |
| spark.sql.shuffle.partitions         | 70     | 配置汇聚时shuffle的分区数                                    |
| spark.executor.instances             | 6      | executor实例的个数，6台服务器每台一个实例                    |
| spark.executor.cores                 | 13     | 每一个实例的核数，这里配置13核                               |
| spark.locality.wait                  | 0      | 配置数据本地化的等待时间为不等待                             |
| spark.executor.memory                | 30G    | executor的内存配置                                           |
| spark.driver.cores                   | 3      | driver程序的CPU内核数量，设置为3                             |
| spark.driver.memory                  | 50G    | driver进程使用的内存数                                       |
| spark.sql.codegen.wholeStage         | True   | 打开codegen开关，该开关默认也是开启的                        |
| spark.sql.codegen.hugeMethodLimit    | 8000   | codegen应用的方法的长度限制，这里应该配置的与JDK相同         |



## 4. 不同数量量的查询性能对比

某商业列存DB与CarbonData的查询均为取多次求平均值。

| 表的分类：数据量+counter个数 | 表记录数（条） | counter 个数 | 某商业列存DB 5次 查询平均耗时（s） | CarbonData 5次查询平均耗时（s） |
| ---------------------------- | -------------- | ------------ | ---------------------------------- | ------------------------------- |
| 100K_9Counter                | 100K           | 9Counter     | 0.91                               | 3.53                            |
| 100K_18Counter               | 100K           | 18Counter    | 1.30                               | 3.81                            |
| 100K_36Counter               | 100K           | 36Counter    | 1.87                               | 4.29                            |
| 100K_72Counter               | 100K           | 72Counter    | 3.82                               | 5.09                            |
| 500K_9Counter                | 500K           | 9Counter     | 1.47                               | 4.04                            |
| 500K_18Counter               | 500K           | 18Counter    | 1.98                               | 4.61                            |
| 500K_36Counter               | 500K           | 36Counter    | 2.99                               | 5.63                            |
| 500K_72Counter               | 500K           | 72Counter    | 5.67                               | 7.53                            |
| 1M_9Counter                  | 1M             | 9Counter     | 4.72                               | 4.24                            |
| 1M_18Counter                 | 1M             | 18Counter    | 5.13                               | 4.84                            |
| 1M_36Counter                 | 1M             | 36Counter    | 6.55                               | 5.83                            |
| 1M_72Counter                 | 1M             | 72Counter    | 10.83                              | 7.90                            |
| 5M_9Counter                  | 5M             | 9Counter     | 5.82                               | 4.59                            |
| 5M_18Counter                 | 5M             | 18Counter    | 7.70                               | 5.26                            |
| 5M_36Counter                 | 5M             | 36Counter    | 11.32                              | 6.73                            |
| 5M_72Counter                 | 5M             | 72Counter    | 21.78                              | 9.27                            |
| 10M_9Counter                 | 10M            | 9Counter     | 7.98                               | 5.32                            |
| 10M_18Counter                | 10M            | 18Counter    | 11.39                              | 6.03                            |
| 10M_36Counter                | 10M            | 36Counter    | 17.40                              | 7.43                            |
| 10M_72Counter                | 10M            | 72Counter    | 34.50                              | 10.48                           |
| 50M_9Counter                 | 50M            | 9Counter     | 16.89                              | 8.95                            |
| 50M_18Counter                | 50M            | 18Counter    | 25.50                              | 10.42                           |
| 50M_36Counter                | 50M            | 36Counter    | 268.10                             | 12.78                           |
| 50M_72Counter                | 50M            | 72Counter    | 554.16                             | 18.79                           |
| 100M_9Counter                | 100M           | 9Counter     | 25.13                              | 13.19                           |
| 100M_18Counter               | 100M           | 18Counter    | 35.57                              | 14.87                           |
| 100M_36Counter               | 100M           | 36Counter    | 299.43                             | 18.96                           |
| 100M_72Counter               | 100M           | 72Counter    | 678.72                             | 28.12                           |
| 1B_9Counter                  | 1B             | 9Counter     | 167.50                             | 47.95                           |
| 1B_18Counter                 | 1B             | 18Counter    | 261.20                             | 55.79                           |
| 1B_36Counter                 | 1B             | 36Counter    | 654.99                             | 73.14                           |
| 1B_72Counter                 | 1B             | 72Counter    | 1575.81                            | 116.63                          |



## 5. 总结

通过上面的测试结果可以看出：
1. 在同等CPU内存资源及使用SATA盘劣势资源的情况下，CarbonData的查询性能要高于某商业列存DB。
2. 在百万级及以上数据量的查询中CarbonData的查询性能明显高于商业列存DB，整体查询性能有了较高的提升，平均查询性能提升1.5-10倍。
3. 在百万级数据以上，随着数据量的增大，CarbonData的查询优势越来越明显。