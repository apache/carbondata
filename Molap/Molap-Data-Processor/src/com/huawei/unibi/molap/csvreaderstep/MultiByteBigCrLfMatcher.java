package com.huawei.unibi.molap.csvreaderstep;

public class MultiByteBigCrLfMatcher implements CrLfMatcherInterface {

  @Override
  public boolean isLineFeed(byte[] source, int location) {
    if (location >= 1) {
      return source[location - 1] == 0 && source[location] == 0x0a;
    } else {
      return false;
    }
  }

  @Override
  public boolean isReturn(byte[] source, int location) {
    if (location >= 1) {
      return source[location - 1] == 0 && source[location] == 0x0d;
    } else {
      return false;
    }
  }

}