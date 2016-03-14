package com.huawei.unibi.molap.csvreaderstep;

public class EmptyPatternMatcher implements PatternMatcherInterface {

  public boolean matchesPattern(byte[] source, int location, byte[] pattern) {
    return false;
  }
  
}
