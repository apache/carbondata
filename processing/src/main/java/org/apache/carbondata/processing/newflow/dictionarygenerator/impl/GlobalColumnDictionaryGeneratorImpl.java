package org.apache.carbondata.processing.newflow.dictionarygenerator.impl;

import org.apache.carbondata.processing.newflow.dictionarygenerator.ColumnDictionaryGenerator;

/**
 * It can generates global dictionary using external KV store or distributed map.
 */
public class GlobalColumnDictionaryGeneratorImpl implements ColumnDictionaryGenerator {

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
