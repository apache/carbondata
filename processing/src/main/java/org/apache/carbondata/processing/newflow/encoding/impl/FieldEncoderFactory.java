package org.apache.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;

import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;

public class FieldEncoderFactory {

  private static FieldEncoderFactory instance;

  private FieldEncoderFactory() {

  }

  public static FieldEncoderFactory getInstance() {
    if (instance == null) {
      instance = new FieldEncoderFactory();
    }
    return instance;
  }

  public FieldEncoder createFieldEncoder(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, int index) {
    if (dataField.hasDictionaryEncoding()) {
      return new DictionaryFieldEncoderImpl(dataField, cache, carbonTableIdentifier, index);
    } else if (dataField.getColumn().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      return new DirectDictionaryFieldEncoderImpl(dataField, index);
    } else if (dataField.getColumn().isComplex()) {
      return new ComplexFieldEncoderImpl();
    } else if ((dataField.getColumn().hasEncoding(Encoding.DICTIONARY) || dataField.getColumn()
        .hasEncoding(Encoding.DIRECT_DICTIONARY))) {
      return new NonDictionaryFieldEncoderImpl(dataField, index);
    } else if (!dataField.getColumn().isDimesion()) {
      return new MeasureFieldEncoderImpl(dataField, index);
    }
    return new NonDictionaryFieldEncoderImpl(dataField, index);
  }
}
