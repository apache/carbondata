package org.apache.carbondata.processing.newflow.parser.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.carbondata.processing.newflow.complexobjects.StructObject;
import org.apache.carbondata.processing.newflow.parser.GenericParser;

public class StructParserImpl implements GenericParser<StructObject> {

  private Pattern pattern;

  private List<GenericParser> children = new ArrayList<>();

  public StructParserImpl(String delimiter) {
    pattern = Pattern.compile(CarbonUtil.delimiterConverter(delimiter));
  }

  @Override public StructObject parse(String data) {
    if (StringUtils.isNotEmpty(data)) {
      String[] split = pattern.split(data, -1);
      if (ArrayUtils.isNotEmpty(split)) {
        Object[] array = new Object[children.size()];
        for (int i = 0; i < children.size(); i++) {
          array[i] = children.get(i).parse(split[i]);
        }
        return new StructObject(array);
      }
    }
    return null;
  }

  @Override public void addChildren(GenericParser parser) {
    children.add(parser);
  }
}
