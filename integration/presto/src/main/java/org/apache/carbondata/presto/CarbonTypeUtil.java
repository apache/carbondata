package org.apache.carbondata.presto;

import org.apache.carbondata.core.metadata.datatype.DataType;

import org.apache.spark.sql.types.DataTypes;

public class CarbonTypeUtil {

  public static org.apache.spark.sql.types.DataType convertCarbonToSparkDataType(
      DataType carbonDataType) {
    switch (carbonDataType) {
      case STRING:
        return DataTypes.StringType;
      case SHORT:
        return DataTypes.ShortType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case DECIMAL:
        return DataTypes.createDecimalType();
      case TIMESTAMP:
        return DataTypes.TimestampType;
      case DATE:
        return DataTypes.DateType;
      default: return null;
    }
  }

}
