package org.apache.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;

import org.apache.carbondata.processing.newflow.DataField;

public class DirectDictionaryFieldEncoderImpl extends AbstractDictionaryFieldEncoderImpl {

  private DirectDictionaryGenerator directDictionaryGenerator;

  private int index;

  public DirectDictionaryFieldEncoderImpl(DataField dataField, int index) {
    DirectDictionaryGenerator directDictionaryGenerator =
        DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dataField.getColumn().getDataType());
    this.directDictionaryGenerator = directDictionaryGenerator;
    this.index = index;
  }

  @Override public Integer encode(Object[] data) {
    return directDictionaryGenerator.generateDirectSurrogateKey((String) data[index]);
  }

  @Override public int getColumnCardinality() {
    return Integer.MAX_VALUE;
  }
}
