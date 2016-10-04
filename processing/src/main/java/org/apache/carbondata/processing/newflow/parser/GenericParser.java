package org.apache.carbondata.processing.newflow.parser;

public interface GenericParser<E> {

  E parse(String data);

  void addChildren(GenericParser parser);

}
