package org.apache.carbondata.processing.newflow.dictionarygenerator;

/**
 * Generates dictionary for the column. The implementation classes can be pre-defined or
 * local or global dictionary generations.
 */
public interface ColumnDictionaryGenerator {

  /**
   * Generates dictionary value for the column data
   * @param data
   * @return dictionary value
   */
  int generateDictionaryValue(Object data);

  /**
   * Returns the actual value associated with dictionary value.
   * @param dictionary
   * @return actual value.
   */
  Object getValueFromDictionary(int dictionary);

  /**
   * Returns the maximum value among the dictionary values. It is used for generating mdk key.
   * @return max dictionary value.
   */
  int getMaxDictionaryValue();

}
