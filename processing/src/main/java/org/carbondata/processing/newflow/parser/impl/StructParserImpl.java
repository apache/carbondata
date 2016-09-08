package org.carbondata.processing.newflow.parser.impl;

import java.util.regex.Pattern;

import org.carbondata.processing.newflow.complexobjects.StructObject;
import org.carbondata.processing.newflow.parser.GenericParser;

public class StructParserImpl implements GenericParser<StructObject> {

  private Pattern pattern;

  public StructParserImpl(char delimiter) {

  }

  @Override public StructObject parse(String data) {

    return null;
  }

  @Override public void addChildren(GenericParser parser) {

  }
}
