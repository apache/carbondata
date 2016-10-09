package org.apache.carbondata.processing.newflow.encoding.impl;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonUtilException;

import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.dictionarygenerator.ColumnDictionaryGenerator;
import org.apache.carbondata.processing.newflow.dictionarygenerator.impl.PreGeneratedColumnDictionaryGeneratorImpl;

public class DictionaryFieldEncoderImpl extends AbstractDictionaryFieldEncoderImpl {

  private ColumnDictionaryGenerator dictionaryGenerator;

  private int index;

  private int cardinality = Integer.MAX_VALUE;

  public DictionaryFieldEncoderImpl(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, int index) {
    this.index = index;
    DictionaryColumnUniqueIdentifier identifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            dataField.getColumn().getColumnIdentifier(), dataField.getColumn().getDataType());
    try {
      Dictionary dictionary = cache.get(identifier);
      dictionaryGenerator = new PreGeneratedColumnDictionaryGeneratorImpl(dictionary);
      cardinality = dictionary.getDictionaryChunks().getSize();
    } catch (CarbonUtilException e) {
      e.printStackTrace();
    }
  }

  @Override public Integer encode(Object[] data) {
    return this.dictionaryGenerator.generateDictionaryValue(data[index]);
  }

  @Override public int getColumnCardinality() {
    return cardinality;
  }
}
