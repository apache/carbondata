package org.carbondata.processing.newflow.compression;

import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.CompressionCodec;

import org.carbondata.processing.newflow.compression.impl.SnappyHeavyCompressorImpl;

public class HeavyCompressorFactory {

  private HeavyCompressorFactory() {

  }
  public static HeavyCompressor createHeavyCompressor(CompressionCodec compressionCodec) {
    switch (compressionCodec) {
      case SNAPPY :
        return new SnappyHeavyCompressorImpl();
      default:
        return new SnappyHeavyCompressorImpl();
    }
  }
}
