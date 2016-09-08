package org.carbondata.processing.newflow.compression;

public interface HeavyCompressor {

  byte[] compress(byte[] data);

  byte[] compress(short[] data);

  byte[] compress(int[] data);

  byte[] compress(long[] data);

  byte[] compress(float[] data);

  byte[] compress(double[] data);

  byte[] uncompress(byte[] data);

  short[] uncompressShortArray(byte[] data);

  int[] uncompressIntArray(byte[] data);

  long[] uncompressLongArray(byte[] data);

  float[] uncompressFloatArray(byte[] data);

  double[] uncompressDoubleArray(byte[] data);

}
