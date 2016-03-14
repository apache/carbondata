package com.huawei.unibi.molap.csvreaderstep;

public interface PatternMatcherInterface {
  boolean matchesPattern(byte[] source, int location, byte[] pattern);
}
