package org.apache.carbondata.processing.newflow.dictionarygenerator.impl;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.processing.newflow.dictionarygenerator.ColumnDictionaryGenerator;

/**
 * It just fetches already generated dictionary values.
 */
public class PreGeneratedColumnDictionaryGeneratorImpl implements ColumnDictionaryGenerator {

  private Dictionary dictionary;

  public PreGeneratedColumnDictionaryGeneratorImpl(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override public int generateDictionaryValue(Object data) {
    return dictionary.getSurrogateKey(data.toString());
  }

  @Override public Object getValueFromDictionary(int dictionaryValue) {
    return dictionary.getDictionaryValueForKey(dictionaryValue);
  }

  @Override public int getMaxDictionaryValue() {
    return dictionary.getDictionaryChunks().getSize();
  }
}
