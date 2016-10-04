package org.apache.carbondata.processing.newflow.encoding.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;

public class NonDictionaryFieldEncoderImpl implements FieldEncoder<ByteBuffer> {

  private DataType dataType;

  private int index;

  public NonDictionaryFieldEncoderImpl(DataField dataField, int index) {
    this.dataType = dataField.getColumn().getDataType();
    this.index = index;
  }

  @Override public ByteBuffer encode(Object[] data) {
    String dimensionValue = (String) data[index];
    if (dataType != DataType.STRING) {
      if (null == DataTypeUtil.normalizeIntAndLongValues(dimensionValue, dataType)) {
        dimensionValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
      }
    }
    ByteBuffer buffer = ByteBuffer
        .wrap(dimensionValue.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
    buffer.rewind();
    return buffer;
  }
}
