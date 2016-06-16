package org.carbondata.spark.readsupport;

import java.sql.Timestamp;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.hadoop.readsupport.impl.AbstractDictionaryDecodedReadSupport;
import org.carbondata.query.carbon.util.DataTypeUtil;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkRowReadSupportImpl extends AbstractDictionaryDecodedReadSupport<Row> {

  @Override public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    super.intialize(carbonColumns, absoluteTableIdentifier);
    //can intialize and generate schema here.
  }

  @Override public Row readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = DataTypeUtil
            .getDataBasedOnDataType(dictionaries[i].getDictionaryValueForKey((int) data[i]),
                dataTypes[i]);
        switch (dataTypes[i]) {
          case STRING:
            data[i] = UTF8String.fromString(data[i].toString());
            break;
          case TIMESTAMP:
            data[i] = new Timestamp((long) data[i] / 1000);
            break;
          default:
        }
      } else if (carbonColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        //convert the long to timestamp in case of direct dictionary column
        if (DataType.TIMESTAMP == carbonColumns[i].getDataType()) {
          data[i] = new Timestamp((long) data[i] / 1000);
        }
      }
    }
    return new GenericRow(data);
  }
}
