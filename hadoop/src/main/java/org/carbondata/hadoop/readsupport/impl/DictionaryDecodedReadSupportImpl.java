package org.carbondata.hadoop.readsupport.impl;

/**
 * It decodes the dictionary values to actual values.
 */
public class DictionaryDecodedReadSupportImpl
    extends AbstractDictionaryDecodedReadSupport<Object[]> {

  @Override public Object[] readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = dictionaries[i].getDictionaryValueForKey((int) data[i]);
      }
    }
    return data;
  }
}
