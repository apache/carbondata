/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaFloatingCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveFloatingCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressCodec;
import org.apache.carbondata.core.datastore.page.encoding.rle.RLECodec;
import org.apache.carbondata.core.datastore.page.encoding.rle.RLEEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.Encoding;

import static org.apache.carbondata.format.Encoding.ADAPTIVE_DELTA_FLOATING;
import static org.apache.carbondata.format.Encoding.ADAPTIVE_DELTA_INTEGRAL;
import static org.apache.carbondata.format.Encoding.ADAPTIVE_FLOATING;
import static org.apache.carbondata.format.Encoding.ADAPTIVE_INTEGRAL;
import static org.apache.carbondata.format.Encoding.BOOL_BYTE;
import static org.apache.carbondata.format.Encoding.DIRECT_COMPRESS;
import static org.apache.carbondata.format.Encoding.DIRECT_COMPRESS_VARCHAR;
import static org.apache.carbondata.format.Encoding.RLE_INTEGRAL;

/**
 * Base class for encoding factory implementation.
 */
public abstract class EncodingFactory {

  /**
   * Return new encoder for specified column
   */
  public abstract ColumnPageEncoder createEncoder(TableSpec.ColumnSpec columnSpec,
      ColumnPage inputPage);

  /**
   * Return new decoder based on encoder metadata read from file
   */
  public ColumnPageDecoder createDecoder(List<Encoding> encodings, List<ByteBuffer> encoderMetas,
      String compressor) throws IOException {
    return createDecoder(encodings, encoderMetas, compressor, false);
  }

  /**
   * Return new decoder based on encoder metadata read from file
   */
  public ColumnPageDecoder createDecoder(List<Encoding> encodings, List<ByteBuffer> encoderMetas,
      String compressor, boolean fullVectorFill) throws IOException {
    assert (encodings.size() >= 1);
    assert (encoderMetas.size() == 1);
    Encoding encoding = encodings.get(0);
    byte[] encoderMeta = encoderMetas.get(0).array();
    ByteArrayInputStream stream = new ByteArrayInputStream(encoderMeta);
    DataInputStream in = new DataInputStream(stream);
    if (encoding == DIRECT_COMPRESS || encoding == DIRECT_COMPRESS_VARCHAR) {
      ColumnPageEncoderMeta metadata = new ColumnPageEncoderMeta();
      metadata.setFillCompleteVector(fullVectorFill);
      metadata.readFields(in);
      return new DirectCompressCodec(metadata.getStoreDataType()).createDecoder(metadata);
    } else if (encoding == ADAPTIVE_INTEGRAL) {
      ColumnPageEncoderMeta metadata = new ColumnPageEncoderMeta();
      metadata.setFillCompleteVector(fullVectorFill);
      metadata.readFields(in);
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
      return new AdaptiveIntegralCodec(metadata.getSchemaDataType(), metadata.getStoreDataType(),
          stats, encodings.contains(Encoding.INVERTED_INDEX)).createDecoder(metadata);
    } else if (encoding == ADAPTIVE_DELTA_INTEGRAL) {
      ColumnPageEncoderMeta metadata = new ColumnPageEncoderMeta();
      metadata.setFillCompleteVector(fullVectorFill);
      metadata.readFields(in);
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
      return new AdaptiveDeltaIntegralCodec(metadata.getSchemaDataType(),
          metadata.getStoreDataType(), stats, encodings.contains(Encoding.INVERTED_INDEX))
          .createDecoder(metadata);
    } else if (encoding == ADAPTIVE_FLOATING) {
      ColumnPageEncoderMeta metadata = new ColumnPageEncoderMeta();
      metadata.setFillCompleteVector(fullVectorFill);
      metadata.readFields(in);
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
      return new AdaptiveFloatingCodec(metadata.getSchemaDataType(), metadata.getStoreDataType(),
          stats, encodings.contains(Encoding.INVERTED_INDEX)).createDecoder(metadata);
    } else if (encoding == ADAPTIVE_DELTA_FLOATING) {
      ColumnPageEncoderMeta metadata = new ColumnPageEncoderMeta();
      metadata.setFillCompleteVector(fullVectorFill);
      metadata.readFields(in);
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
      return new AdaptiveDeltaFloatingCodec(metadata.getSchemaDataType(),
          metadata.getStoreDataType(), stats, encodings.contains(Encoding.INVERTED_INDEX))
          .createDecoder(metadata);
    } else if (encoding == RLE_INTEGRAL) {
      RLEEncoderMeta metadata = new RLEEncoderMeta();
      metadata.readFields(in);
      return new RLECodec().createDecoder(metadata);
    } else if (encoding == BOOL_BYTE) {
      ColumnPageEncoderMeta metadata = new ColumnPageEncoderMeta();
      metadata.setFillCompleteVector(fullVectorFill);
      metadata.readFields(in);
      return new DirectCompressCodec(metadata.getStoreDataType()).createDecoder(metadata);
    } else {
      // for backward compatibility
      ValueEncoderMeta metadata = CarbonUtil.deserializeEncoderMetaV3(encoderMeta);
      return createDecoderLegacy(metadata, compressor, fullVectorFill);
    }
  }

  /**
   * Old way of creating decoder, based on algorithm
   */
  public ColumnPageDecoder createDecoderLegacy(ValueEncoderMeta metadata, String compressor) {
    return createDecoderLegacy(metadata, compressor, false);
  }

  /**
   * Old way of creating decoder, based on algorithm
   */
  private ColumnPageDecoder createDecoderLegacy(ValueEncoderMeta metadata, String compressor,
      boolean fullVectorFill) {
    if (null == metadata) {
      throw new RuntimeException("internal error");
    }
    SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
    TableSpec.ColumnSpec spec =
        TableSpec.ColumnSpec.newInstanceLegacy("legacy", stats.getDataType(), ColumnType.MEASURE);
    DataType dataType = DataType.getDataType(metadata.getType());
    if (dataType == DataTypes.BYTE ||
        dataType == DataTypes.SHORT ||
        dataType == DataTypes.INT ||
        dataType == DataTypes.LONG) {
      // create the codec based on algorithm and create decoder by recovering the metadata
      ColumnPageCodec codec =
          DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(stats, false, spec);
      if (codec instanceof AdaptiveIntegralCodec) {
        AdaptiveIntegralCodec adaptiveCodec = (AdaptiveIntegralCodec) codec;
        ColumnPageEncoderMeta meta =
            new ColumnPageEncoderMeta(spec, adaptiveCodec.getTargetDataType(), stats, compressor);
        meta.setFillCompleteVector(fullVectorFill);
        return codec.createDecoder(meta);
      } else if (codec instanceof AdaptiveDeltaIntegralCodec) {
        AdaptiveDeltaIntegralCodec adaptiveCodec = (AdaptiveDeltaIntegralCodec) codec;
        ColumnPageEncoderMeta meta =
            new ColumnPageEncoderMeta(spec, adaptiveCodec.getTargetDataType(), stats, compressor);
        meta.setFillCompleteVector(fullVectorFill);
        return codec.createDecoder(meta);
      } else if (codec instanceof DirectCompressCodec) {
        ColumnPageEncoderMeta meta =
            new ColumnPageEncoderMeta(spec, DataType.getDataType(metadata.getType()), stats,
                compressor);
        meta.setFillCompleteVector(fullVectorFill);
        return codec.createDecoder(meta);
      } else {
        throw new RuntimeException("internal error");
      }
    } else if (dataType == DataTypes.FLOAT || dataType == DataTypes.DOUBLE) {
      // create the codec based on algorithm and create decoder by recovering the metadata
      ColumnPageCodec codec =
          DefaultEncodingFactory.selectCodecByAlgorithmForFloating(stats, false, spec);
      if (codec instanceof AdaptiveFloatingCodec) {
        AdaptiveFloatingCodec adaptiveCodec = (AdaptiveFloatingCodec) codec;
        ColumnPageEncoderMeta meta =
            new ColumnPageEncoderMeta(spec, adaptiveCodec.getTargetDataType(), stats, compressor);
        meta.setFillCompleteVector(fullVectorFill);
        return codec.createDecoder(meta);
      } else if (codec instanceof DirectCompressCodec) {
        ColumnPageEncoderMeta meta =
            new ColumnPageEncoderMeta(spec, DataType.getDataType(metadata.getType()), stats,
                compressor);
        meta.setFillCompleteVector(fullVectorFill);
        return codec.createDecoder(meta);
      } else if (codec instanceof AdaptiveDeltaFloatingCodec) {
        AdaptiveDeltaFloatingCodec adaptiveCodec = (AdaptiveDeltaFloatingCodec) codec;
        ColumnPageEncoderMeta meta =
            new ColumnPageEncoderMeta(spec, adaptiveCodec.getTargetDataType(), stats, compressor);
        meta.setFillCompleteVector(fullVectorFill);
        return codec.createDecoder(meta);
      } else {
        throw new RuntimeException("internal error");
      }
    } else if (DataTypes.isDecimal(dataType) || dataType == DataTypes.BYTE_ARRAY) {
      // no dictionary dimension
      ColumnPageEncoderMeta meta =
          new ColumnPageEncoderMeta(spec, stats.getDataType(), stats, compressor);
      meta.setFillCompleteVector(fullVectorFill);
      return new DirectCompressCodec(stats.getDataType()).createDecoder(meta);
    } else if (dataType == DataTypes.LEGACY_LONG) {
      // In case of older versions like in V1 format it has special datatype to handle
      AdaptiveIntegralCodec adaptiveCodec =
          new AdaptiveIntegralCodec(DataTypes.LONG, DataTypes.LONG, stats, false);
      ColumnPageEncoderMeta meta =
          new ColumnPageEncoderMeta(spec, adaptiveCodec.getTargetDataType(), stats, compressor);
      meta.setFillCompleteVector(fullVectorFill);
      return adaptiveCodec.createDecoder(meta);
    } else {
      throw new RuntimeException("unsupported data type: " + stats.getDataType());
    }
  }
}
