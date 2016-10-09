package org.apache.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;

/**
 * Created by root1 on 4/10/16.
 */
public class MeasureFieldEncoderImpl implements FieldEncoder<Object> {

  private int index;

  public MeasureFieldEncoderImpl(DataField dataField, int index) {
    this.index = index;
  }

  @Override public Object encode(Object[] data) {
    return data[index];
  }
}
