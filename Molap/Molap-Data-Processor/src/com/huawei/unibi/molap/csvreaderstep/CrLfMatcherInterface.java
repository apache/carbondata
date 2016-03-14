package com.huawei.unibi.molap.csvreaderstep;

public interface CrLfMatcherInterface {
  boolean isReturn(byte[] source, int location);
  boolean isLineFeed(byte[] source, int location);
}
