package org.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonUtilException;

import org.carbondata.processing.newflow.DataField;
import org.carbondata.processing.newflow.encoding.FieldEncoder;

public class DictionaryFieldEncoderImpl implements FieldEncoder {

  private Dictionary dictionary;

  private int index;

  public DictionaryFieldEncoderImpl(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, int index) {
    this.index = index;
    DictionaryColumnUniqueIdentifier identifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            dataField.getColumn().getColumnIdentifier(), dataField.getColumn().getDataType());
    try {
      this.dictionary = cache.get(identifier);
    } catch (CarbonUtilException e) {
      e.printStackTrace();
    }
  }

  @Override public void encode(Object[] data) {
    data[index] = this.dictionary.getSurrogateKey(data[index].toString());
  }

}
