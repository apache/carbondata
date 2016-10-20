package org.apache.carbondata.processing.newflow.parser;

/**
 * It parses data string as per complex data type.
 */
public interface ComplexParser<E> extends GenericParser<E> {

  /**
   * Children to this parser.
   * @param parser
   */
  void addChildren(GenericParser parser);
}
