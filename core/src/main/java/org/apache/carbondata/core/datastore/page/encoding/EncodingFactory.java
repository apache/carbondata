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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveFloatingCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveFloatingEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveIntegralEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressCodec;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressorEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.directstring.DirectStringCodec;
import org.apache.carbondata.core.datastore.page.encoding.directstring.DirectStringEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.rle.RLECodec;
import org.apache.carbondata.core.datastore.page.encoding.rle.RLEEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.Encoding;

import static org.apache.carbondata.format.Encoding.ADAPTIVE_DELTA_INTEGRAL;
import static org.apache.carbondata.format.Encoding.ADAPTIVE_FLOATING;
import static org.apache.carbondata.format.Encoding.ADAPTIVE_INTEGRAL;
import static org.apache.carbondata.format.Encoding.DIRECT_COMPRESS;
import static org.apache.carbondata.format.Encoding.DIRECT_STRING;
import static org.apache.carbondata.format.Encoding.RLE_INTEGRAL;

/**
 * Base class for encoding strategy implementation.
 */
public abstract class EncodingFactory {

  private static final LogService LOG =
      LogServiceFactory.getLogService(EncodingFactory.class.getCanonicalName());

  /**
   * Return new encoder for specified column
   */
  public abstract ColumnPageEncoder createEncoder(TableSpec.ColumnSpec columnSpec,
      ColumnPage inputPage);

  /**
   * Return new decoder based on encoder metadata read from file
   */
  public ColumnPageDecoder createDecoder(List<Encoding> encodings, List<ByteBuffer> encoderMetas)
      throws IOException {
    assert (encodings.size() == 1);
    assert (encoderMetas.size() == 1);
    Encoding encoding = encodings.get(0);
    byte[] encoderMeta = encoderMetas.get(0).array();
    ByteArrayInputStream stream = new ByteArrayInputStream(encoderMeta);
    DataInputStream in = new DataInputStream(stream);
    ColumnPageCodec codec = null;
    ColumnPageEncoderMeta metadata = null;
    if (encoding == DIRECT_COMPRESS) {
      metadata = new DirectCompressorEncoderMeta();
      metadata.readFields(in);
      codec = new DirectCompressCodec(metadata.getStoreDataType());
    } else if (encoding == ADAPTIVE_INTEGRAL) {
      metadata = new AdaptiveIntegralEncoderMeta();
      metadata.readFields(in);
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
      codec = new AdaptiveIntegralCodec(
          metadata.getSchemaDataType(), metadata.getStoreDataType(), stats);
    } else if (encoding == ADAPTIVE_DELTA_INTEGRAL) {
      metadata = new AdaptiveDeltaIntegralEncoderMeta();
      metadata.readFields(in);
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
      codec = new AdaptiveDeltaIntegralCodec(
          metadata.getSchemaDataType(), metadata.getStoreDataType(), stats);
    } else if (encoding == RLE_INTEGRAL) {
      metadata = new RLEEncoderMeta();
      metadata.readFields(in);
      codec = new RLECodec();
    } else if (encoding == ADAPTIVE_FLOATING) {
      metadata = new AdaptiveFloatingEncoderMeta();
      metadata.readFields(in);
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
      codec = new AdaptiveFloatingCodec(
          metadata.getSchemaDataType(), metadata.getStoreDataType(), stats);
    } else if (encoding == DIRECT_STRING) {
      metadata = new DirectStringEncoderMeta();
      metadata.readFields(in);
      codec = new DirectStringCodec();
    }

    if (codec != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Using " + codec.getName() + " to decode column '" +
            metadata.getColumnSpec().getFieldName() + "'");
      }
      return codec.createDecoder(metadata);
    } else {
      // for backward compatibility
      ValueEncoderMeta meta = CarbonUtil.deserializeEncoderMetaV3(encoderMeta);
      ColumnPageDecoder decoder = createDecoderLegacy(meta);
      if (LOG.isInfoEnabled()) {
        LOG.info("Using " + decoder + " for legacy data");
      }
      return decoder;
    }
  }

  /**
   * Old way of creating decoder, based on algorithm
   */
  public ColumnPageDecoder createDecoderLegacy(ValueEncoderMeta metadata) {
    SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(metadata);
    TableSpec.ColumnSpec spec = new TableSpec.ColumnSpec("legacy", stats.getDataType(),
        ColumnType.MEASURE);
    switch (metadata.getType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        // create the codec based on algorithm and create decoder by recovering the metadata
        ColumnPageCodec codec = DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(stats);
        if (codec instanceof AdaptiveIntegralCodec) {
          AdaptiveIntegralCodec adaptiveCodec = (AdaptiveIntegralCodec) codec;
          AdaptiveIntegralEncoderMeta meta = new AdaptiveIntegralEncoderMeta(spec,
              "snappy", adaptiveCodec.getTargetDataType(), stats);
          return codec.createDecoder(meta);
        } else if (codec instanceof AdaptiveDeltaIntegralCodec) {
          AdaptiveDeltaIntegralCodec adaptiveCodec = (AdaptiveDeltaIntegralCodec) codec;
          AdaptiveDeltaIntegralEncoderMeta meta = new AdaptiveDeltaIntegralEncoderMeta(spec,
              "snappy", adaptiveCodec.getTargetDataType(), stats);
          return codec.createDecoder(meta);
        } else if (codec instanceof DirectCompressCodec) {
          DirectCompressorEncoderMeta meta = new DirectCompressorEncoderMeta(spec,
              "snappy", metadata.getType(), stats);
          return codec.createDecoder(meta);
        } else {
          throw new RuntimeException("internal error");
        }
      case FLOAT:
      case DOUBLE:
        // create the codec based on algorithm and create decoder by recovering the metadata
        codec = DefaultEncodingFactory.selectCodecByAlgorithmForFloating(stats);
        if (codec instanceof AdaptiveFloatingCodec) {
          AdaptiveFloatingCodec adaptiveCodec = (AdaptiveFloatingCodec) codec;
          AdaptiveFloatingEncoderMeta meta = new AdaptiveFloatingEncoderMeta(spec,
              "snappy", adaptiveCodec.getTargetDataType(), stats);
          return codec.createDecoder(meta);
        } else if (codec instanceof DirectCompressCodec) {
          DirectCompressorEncoderMeta meta = new DirectCompressorEncoderMeta(spec,
              "snappy", metadata.getType(), stats);
          return codec.createDecoder(meta);
        } else {
          throw new RuntimeException("internal error");
        }
      case DECIMAL:
      case BYTE_ARRAY:
        return new DirectCompressCodec(stats.getDataType()).createDecoder(
            new DirectCompressorEncoderMeta(spec, "snappy", metadata.getType(), stats));
      default:
        throw new RuntimeException("unsupported data type: " + stats.getDataType());
    }
  }
}
