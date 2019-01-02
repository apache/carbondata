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

# CarbonData Timeseries DataMap

* [Timeseries DataMap Introduction](#timeseries-datamap-introduction-alpha-feature)
* [Compaction](#compacting-timeseries-datamp)
* [Data Management](#data-management-on-timeseries-datamap)

## Timeseries DataMap Introduction (Alpha Feature)
Timeseries DataMap is a pre-aggregate table implementation based on 'pre-aggregate' DataMap.
Difference is that Timeseries DataMap has built-in understanding of time hierarchy and
levels: year, month, day, hour, minute, so that it supports automatic roll-up in time dimension 
for query.

**CAUTION:** Current version of CarbonData does not support roll-up.It will be implemented in future versions.

The data loading, querying, compaction command and its behavior is the same as preaggregate DataMap.
Please refer to [Pre-aggregate DataMap](./preaggregate-datamap-guide.md)
for more information.
  
To use this datamap, user can create multiple timeseries datamap on the main table which has 
a *event_time* column, one datamap for one time granularity.

For example, below statement effectively create multiple pre-aggregate tables  on main table called 
**timeseries**

```
CREATE DATAMAP agg_year
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'year_granularity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
  
CREATE DATAMAP agg_month
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'month_granularity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
  
CREATE DATAMAP agg_day
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'day_granularity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
      
CREATE DATAMAP agg_sales_hour
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'hour_granularity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex

CREATE DATAMAP agg_minute
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'minute_granularity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
```
  
For querying timeseries data, Carbondata has builtin support for following time related UDF 

```
timeseries(timeseries column name, 'aggregation level')
```
```
SELECT timeseries(order_time, 'hour'), sum(quantity) FROM sales GROUP BY timeseries(order_time,'hour')
```
  
It is **not necessary** to create pre-aggregate tables for each granularity unless required for 
query.
 
For Example: For main table **sales** , if following timeseries datamaps were created for day 
level and hour level pre-aggregate
  
```
CREATE DATAMAP agg_day
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'day_granularity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
      
CREATE DATAMAP agg_sales_hour
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'hour_granularity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
```

Queries like below will not be rolled-up and hit the main table
```
Select timeseries(order_time, 'month'), sum(quantity) from sales group by timeseries(order_time,
  'month')
  
Select timeseries(order_time, 'year'), sum(quantity) from sales group by timeseries(order_time,
  'year')
```

NOTE (**RESTRICTION**):
* Only value of 1 is supported for hierarchy levels. Other hierarchy levels will be supported in
the future CarbonData release. 
* timeseries datamap for the desired levels needs to be created one after the other
* timeseries datamaps created for each level needs to be dropped separately 
      

## Compacting timeseries datamp
Refer to Compaction section in [preaggregation datamap](./preaggregate-datamap-guide.md). 
Same applies to timeseries datamap.

## Data Management on timeseries datamap
Refer to Data Management section in [preaggregation datamap](./preaggregate-datamap-guide.md).
Same applies to timeseries datamap.

