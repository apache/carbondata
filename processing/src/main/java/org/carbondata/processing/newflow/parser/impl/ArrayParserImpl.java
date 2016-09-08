package org.carbondata.processing.newflow.parser.impl;

import java.util.regex.Pattern;

import org.carbondata.processing.newflow.complexobjects.ArrayObject;
import org.carbondata.processing.newflow.parser.GenericParser;

public class ArrayParserImpl implements GenericParser<ArrayObject> {

  private Pattern pattern;

  public ArrayParserImpl(char delimiter) {

  }

  @Override public ArrayObject parse(String data) {
    return null;
  }

  @Override public void addChildren(GenericParser parser) {

  }
}
