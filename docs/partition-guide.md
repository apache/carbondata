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

# CarbonData Partition Table Guidance
This guidance illustrates how to create & use partition table in CarbonData.

* [Create Partition Table](#create-partition-table)
  - [Create Hash Partition Table](#create-hash-partition-table)
  - [Create Range Partition Table](#create-range-partition-table)
  - [Create List Partition Table](#create-list-partition-table)
* [Show Partitions](#show-partitions)
* [Maintain the Partitions](#maintain-the-partitions)
* [Partition Id](#partition-id)
* [Tips](#tips)

## Create Partition Table

### Create Hash Partition Table

```
   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
   PARTITIONED BY (partition_col_name data_type)
   STORED BY 'carbondata'
   [TBLPROPERTIES ('PARTITION_TYPE'='HASH',
                   'PARTITION_NUM'='N' ...)]
   //N is the number of hash partitions
```

Example:

```
   create table if not exists hash_partition_table(
      col_A String,
      col_B Int,
      col_C Long,
      col_D Decimal(10,2),
      col_F Timestamp
   ) partitioned by (col_E Long)
   stored by 'carbondata'
   tblproperties('partition_type'='Hash','partition_num'='9')
```

### Create Range Partition Table

```
   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
   PARTITIONED BY (partition_col_name data_type)
   STORED BY 'carbondata'
   [TBLPROPERTIES ('PARTITION_TYPE'='RANGE',
                   'RANGE_INFO'='2014-01-01, 2015-01-01, 2016-01-01' ...)]
```
Notes:
1. The 'RANGE_INFO' defined in table properties must be in ascending order.
2. If the partition column is Date/Timestamp type, the format could be defined in CarbonProperties. By default it's yyyy-MM-dd.

Example:

```
   create table if not exists hash_partition_table(
      col_A String,
      col_B Int,
      col_C Long,
      col_D Decimal(10,2),
      col_E Long
   ) partitioned by (col_F Timestamp)
   stored by 'carbondata'
   tblproperties('partition_type'='Range',
   'range_info'='2015-01-01, 2016-01-01, 2017-01-01, 2017-02-01')
```

### Create List Partition Table

```
   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
   PARTITIONED BY (partition_col_name data_type)
   STORED BY 'carbondata'
   [TBLPROPERTIES ('PARTITION_TYPE'='LIST',
                   'LIST_INFO'='A, B, C' ...)]
```
Notes:
1. List partition support list info in one level group.

Example：

```
   create table if not exists hash_partition_table(
      col_B Int,
      col_C Long,
      col_D Decimal(10,2),
      col_E Long,
      col_F Timestamp
   ) partitioned by (col_A String)
   stored by 'carbondata'
   tblproperties('partition_type'='List',
   'list_info'='aaaa, bbbb, (cccc, dddd), eeee')
```


## Show Partitions
Execute following command to get the partition information

```
   SHOW PARTITIONS [db_name.]table_name
```

## Maintain the Partitions
### Add a new partition

```
   ALTER TABLE [db_name].table_name ADD PARTITION('new_partition')
```
### Split a partition

```
   ALTER TABLE [db_name].table_name SPLIT PARTITION(partition_id)
   INTO('new_partition1', 'new_partition2'...)
```

### Drop a partition

```
   //Drop partition definition only and keep data
   ALTER TABLE [db_name].table_name DROP PARTITION(partition_id)

   //Drop both partition definition and data
   ALTER TABLE [db_name].table_name DROP PARTITION(partition_id) WITH DATA
```
Notes:
1. For the 1st case(keep data),
   * if the table is a range partition table, data will be merged into the next partition, and if the dropped partition is the last one, then data will be merged into default partition.
   * if the table is a list partition table, data will be merged into default partition.
2. Drop default partition is not allowed, but you can use DELETE statement to delete data in default partition.
3. partition_id could be got from SHOW PARTITIONS command.
4. Hash partition table is not supported for the ADD, SPLIT, DROP command.

## Partition Id
In Carbondata, we don't use folders to divide partitions(just like hive did), instead we use partition id to replace the task id.
It could make use of the characteristic and meanwhile reduce some metadata.

```
SegmentDir/0_batchno0-0-1502703086921.carbonindex
           ^
SegmentDir/part-0-0_batchno0-0-1502703086921.carbondata
                  ^
```

## Tips
Here are some tips to improve query performance of carbon partition table:

* **Do some analysis before choose the proper partition column**

The distribution of data on some column could be very skew, building a skewed partition table is meaningless, so do some basic statistic analysis to avoid creating partition table on an extremely skewed column.

* **Exclude partition column from sort columns**

If you have many dimensions need to be sorted, then exclude partition column from sort columns, that will put other dimensions in a better position of sorting.

* **Remember to add filter on partition column when writing SQLs**

When writing SQLs on partition table, try to use filters on partition column.
