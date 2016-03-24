#drop table carbon
#create cube carbon dimensions(column1 string in row,column2 string in row,column3 string,column4 string in row,column5 string, column6 string,column7 string,column8 string,column9 string,column10 string) measures(measure1 numeric,measure2 numeric,measure3 numeric,measure4 numeric) OPTIONS (HIGH_CARDINALITY_DIMS(column10),PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' columns= (column1) PARTITION_COUNT=1])
#LOAD DATA FACT FROM 'D:\\github\\carbondata\\hybrid_store\\CI\\FeatureTest\\input\\10dim_4msr.csv' INTO Cube carbon partitionData(DELIMITER ',' ,QUOTECHAR '"', FILEHEADER 'column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')
#select * from carbon
select column7,column10 from carbon where column10='column101'
#HIGH_CARDINALITY_DIMS(column10),
#column1,column3,column5,column4,column7,