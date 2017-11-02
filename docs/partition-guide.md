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

# CarbonData Partition Table Guide
This tutorial is designed to provide a quick introduction to create and use partition table in Apache CarbonData.

* [Create Partition Table](#create-partition-table)
  - [Create Hash Partition Table](#create-hash-partition-table)
  - [Create Range Partition Table](#create-range-partition-table)
  - [Create List Partition Table](#create-list-partition-table)
* [Show Partitions](#show-partitions)
* [Maintaining the Partitions](#maintaining-the-partitions)
* [Partition Id](#partition-id)
* [Useful Tips](#useful-tips)

## Create Partition Table

### Create Hash Partition Table

```
   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
   PARTITIONED BY (partition_col_name data_type)
   STORED BY 'carbondata'
   [TBLPROPERTIES ('PARTITION_TYPE'='HASH',
                   'NUM_PARTITION'='N' ...)]
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
   tblproperties('partition_type'='Hash','num_partition'='9')
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

**Note:**

- The 'RANGE_INFO' must be defined in ascending order in the table properties.

- The default format for partition column of Date/Timestamp type is yyyy-MM-dd. Alternate formats for Date/Timestamp could be defined in CarbonProperties.

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
**Note :**
- List partition supports list info in one level group.

Example:

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
The following command is executed to get the partition information of the table

```
   SHOW PARTITIONS [db_name.]table_name
```

## Maintaining the Partitions
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

**Note**:

- In the first case where the data in the table is preserved there can be multiple scenarios as described below :

   * if the table is a range partition table, data will be merged into the next partition, and if the dropped partition is the last partition, then data will be merged into the default partition.

   * if the table is a list partition table, data will be merged into default partition.

- Dropping the default partition is not allowed, but DELETE statement can be used to delete data in default partition.

- The partition_id could be fetched using the [SHOW PARTITIONS](#show-partitions) command.

- Hash partition table is not supported for ADD, SPLIT and DROP commands.

## Partition Id
In CarbonData like the hive, folders are not used to divide partitions instead partition id is used to replace the task id. It could make use of the characteristic and meanwhile reduce some metadata.

```
SegmentDir/0_batchno0-0-1502703086921.carbonindex
           ^
SegmentDir/part-0-0_batchno0-0-1502703086921.carbondata
                  ^
```

## Useful Tips
Here are some useful tips to improve query performance of carbonData partition table:

**Prior analysis of proper partition column**

The distribution of data based on some random column could be skewed, building a skewed partition table is meaningless. Some basic statistical analysis before the creation of partition table can avoid an extremely skewed column.

**Exclude partition column from sort columns**

If you have many dimensions, that need to be sorted then one must exclude column present in the partition from sort columns, this will allow another dimension to do the efficient sorting.

**Remember to add filter on partition column when writing SQL**

When writing SQL on a partition table, try to use filters on the partition column.
