package org.apache.carbondata.processing.newflow.dictionarygenerator.impl;

import org.apache.carbondata.processing.newflow.dictionarygenerator.ColumnDictionaryGenerator;

/**
 * It generates dictionary only for this instance.
 */
public class LocalColumnDictionaryGeneratorImpl implements ColumnDictionaryGenerator {

  @Override public int generateDictionaryValue(Object data) {
    return 0;
  }

  @Override public Object getValueFromDictionary(int dictionary) {
    return null;
  }

  @Override public int getMaxDictionaryValue() {
    return 0;
  }
}
