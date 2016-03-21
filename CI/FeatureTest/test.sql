drop cube carbon
create cube carbon dimensions(imei string in row,protocol string in row,MAC string,City string) OPTIONS (PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=1])
LOAD DATA FACT FROM 'D:\\github\\carbon\\hybrid_store\\CI\\FeatureTest\\input\\dim4.csv' INTO Cube carbon partitionData(DELIMITER ',' ,QUOTECHAR '"', FILEHEADER 'imei,protocol,MAC,City')
select imei,MAC from carbon where MAC='mac20'