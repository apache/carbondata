# CarbonData Timeseries DataMap

* [Timeseries DataMap](#timeseries-datamap-intoduction-(alpha-feature-in-1.3.0))
* [Compaction](#compacting-pre-aggregate-tables)
* [Data Management](#data-management-with-pre-aggregate-tables)

## Timeseries DataMap Intoduction (Alpha feature in 1.3.0)
Timeseries DataMap a pre-aggregate table implementation based on 'preaggregate' DataMap. 
Difference is that Timerseries DataMap has built-in understanding of time hierarchy and 
levels: year, month, day, hour, minute, so that it supports automatic roll-up in time dimension 
for query.

The data loading, querying, compaction command and its behavior is the same as preaggregate DataMap.
Please refer to [Pre-aggregate DataMap](https://github.com/apache/carbondata/blob/master/docs/datamap/preaggregate-datamap-guide.md)
for more information.
  
To use this datamap, user can create multiple timeseries datamap on the main table which has 
a *event_time* column, one datamap for one time granularity. Then Carbondata can do automatic 
roll-up for queries on the main table.

For example, below statement effectively create multiple pre-aggregate tables  on main table called 
**timeseries**

```
CREATE DATAMAP agg_year
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'year_granualrity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
  
CREATE DATAMAP agg_month
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'month_granualrity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
  
CREATE DATAMAP agg_day
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'day_granualrity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
      
CREATE DATAMAP agg_sales_hour
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'hour_granualrity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex

CREATE DATAMAP agg_minute
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'minute_granualrity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
  
CREATE DATAMAP agg_minute
ON TABLE sales
USING "timeseries"
DMPROPERTIES (
  'event_time'='order_time',
  'minute_granualrity'='1',
) AS
SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
 avg(price) FROM sales GROUP BY order_time, country, sex
```
  
For querying timeseries data, Carbondata has builtin support for following time related UDF 
to enable automatically roll-up to the desired aggregation level
```
timeseries(timeseries column name, 'aggregation level')
```
```
SELECT timeseries(order_time, 'hour'), sum(quantity) FROM sales GROUP BY timeseries(order_time,
'hour')
```
  
It is **not necessary** to create pre-aggregate tables for each granularity unless required for 
query. Carbondata can roll-up the data and fetch it.
 
For Example: For main table **sales** , if following timeseries datamaps were created for day 
level and hour level pre-aggregate
  
```
  CREATE DATAMAP agg_day
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
    'event_time'='order_time',
    'day_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
        
  CREATE DATAMAP agg_sales_hour
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
    'event_time'='order_time',
    'hour_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
```

Queries like below will be rolled-up and hit the timeseries datamaps
```
Select timeseries(order_time, 'month'), sum(quantity) from sales group by timeseries(order_time,
  'month')
  
Select timeseries(order_time, 'year'), sum(quantity) from sales group by timeseries(order_time,
  'year')
```

NOTE (<b>RESTRICTION</b>):
* Only value of 1 is supported for hierarchy levels. Other hierarchy levels will be supported in
the future CarbonData release. 
* timeseries datamap for the desired levels needs to be created one after the other
* timeseries datamaps created for each level needs to be dropped separately 
      

## Compacting timeseries datamp
Refer to Compaction section in [preaggregation datamap](https://github.com/apache/carbondata/blob/master/docs/datamap/preaggregate-datamap-guide.md). 
Same applies to timeseries datamap.

## Data Management on timeseries datamap
Refer to Data Management section in [preaggregation datamap](https://github.com/apache/carbondata/blob/master/docs/datamap/preaggregate-datamap-guide.md).
Same applies to timeseries datamap.