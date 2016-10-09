package org.apache.carbondata.processing.newflow.parser;

/**
 * Parse the data according to implementation, The implementation classes can be struct, array or
 * map datatypes.
 */
public interface GenericParser<E> {

  /**
   * Parse the data as per the delimiter
   * @param data
   * @return
   */
  E parse(String data);

  /**
   * Children of the parser.
   * @param parser
   */
  void addChildren(GenericParser parser);

}
