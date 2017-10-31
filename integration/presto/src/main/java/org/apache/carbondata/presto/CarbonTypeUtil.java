package org.apache.carbondata.presto;

import org.apache.carbondata.core.metadata.datatype.DataType;

import org.apache.spark.sql.types.DataTypes;

public class CarbonTypeUtil {

  static org.apache.spark.sql.types.DataType convertCarbonToSparkDataType(
      DataType carbonDataType) {
    if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.STRING) {
      return DataTypes.StringType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT) {
      return DataTypes.ShortType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT) {
      return DataTypes.IntegerType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
        return DataTypes.LongType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE) {
        return DataTypes.DoubleType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN) {
        return DataTypes.BooleanType;
    } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(carbonDataType)) {
        return DataTypes.createDecimalType();
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
        return DataTypes.TimestampType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
      return DataTypes.DateType;
    } else {
      return null;
    }
  }

}
