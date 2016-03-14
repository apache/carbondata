package com.huawei.unibi.molap.csvreaderstep;

public class SingleBytePatternMatcher implements PatternMatcherInterface {

  public boolean matchesPattern(byte[] source, int location, byte[] pattern) {
    return source[location] == pattern[0];
  }
  
}
