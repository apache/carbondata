package org.apache.carbondata.processing.newflow.parser.impl;

import org.apache.carbondata.processing.newflow.parser.GenericParser;

public class PrimitiveParserImpl implements GenericParser<Object> {

  @Override public Object parse(String data) {
    return data;
  }

  @Override public void addChildren(GenericParser parser) {
    // No implementation
  }
}
