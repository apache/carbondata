package org.carbondata.processing.newflow.compression.impl;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.carbondata.processing.newflow.compression.HeavyCompressor;
import org.xerial.snappy.Snappy;

public class SnappyHeavyCompressorImpl implements HeavyCompressor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SnappyHeavyCompressorImpl.class.getName());

  @Override public byte[] compress(byte[] data) {
    try {
      return Snappy.compress(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] compress(short[] data) {
    try {
      return Snappy.compress(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] compress(int[] data) {
    try {
      return Snappy.compress(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] compress(long[] data) {
    try {
      return Snappy.compress(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] compress(float[] data) {
    try {
      return Snappy.compress(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] compress(double[] data) {
    try {
      return Snappy.compress(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return null;
    }
  }

  @Override public byte[] uncompress(byte[] data) {
    try {
      return Snappy.uncompress(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return data;
  }

  @Override public short[] uncompressShortArray(byte[] data) {
    try {
      return Snappy.uncompressShortArray(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public int[] uncompressIntArray(byte[] data) {
    try {
      return Snappy.uncompressIntArray(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public long[] uncompressLongArray(byte[] data) {
    try {
      return Snappy.uncompressLongArray(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public float[] uncompressFloatArray(byte[] data) {
    try {
      return Snappy.uncompressFloatArray(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }

  @Override public double[] uncompressDoubleArray(byte[] data) {
    try {
      return Snappy.uncompressDoubleArray(data);
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return null;
  }
}
