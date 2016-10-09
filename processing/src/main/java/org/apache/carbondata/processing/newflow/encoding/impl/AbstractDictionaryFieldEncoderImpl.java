package org.apache.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;

public abstract class AbstractDictionaryFieldEncoderImpl implements FieldEncoder<Integer> {

  public abstract int getColumnCardinality();

}
