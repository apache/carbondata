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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.columnar.PrimitivePageIndexGenerator;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

/**
 * Codec class for primitive data type columns
 */
public class PrimitiveTypeColumnCodec extends IndexStorageCodec {

  private DataType dataType;

  private List<Encoding> encodings;

  public PrimitiveTypeColumnCodec(boolean isSort, DataType dataType, List<Encoding> encodings) {
    super(isSort);
    this.dataType = dataType;
    this.encodings = encodings;
  }

  @Override public String getName() {
    return "PrimitiveTypeColumnCodec";
  }

  @Override public ColumnPageEncoder createEncoder(Map<String, Object> parameter) {
    return new IndexStorageEncoder(false, null == parameter ?
        null :
        (parameter.get("keygenerator") == null ?
            null :
            (KeyGenerator) parameter.get("keygenerator")), encodings) {
      @Override protected void encodeIndexStorage(ColumnPage input)
          throws MemoryException, IOException {
        // for local dictionary generated column use local dict page
        ColumnPage actualPage =
            !input.isLocalDictGeneratedPage() ? input : input.getLocalDictPage();
        PageIndexGenerator<Object[]> pageIndexGenerator;
        // get data
        Object[] data = actualPage.getPageBasedOnDataType();
        pageIndexGenerator =
            new PrimitivePageIndexGenerator(data, isSort, actualPage.getDataType());
        // create a page for applying adaptive encoding
        ColumnPage adaptivePage;
        TableSpec.MeasureSpec spec =
            TableSpec.MeasureSpec.newInstance(actualPage.getColumnSpec().getFieldName(), dataType);
        adaptivePage = ColumnPage
            .newPage(new ColumnPageEncoderMeta(spec, dataType, input.getColumnCompressorName()),
                actualPage.getPageSize());
        // get updated data from index generator
        Object[] dataPage = pageIndexGenerator.getDataPage();
        // adding stats collector
        adaptivePage.setStatsCollector(PrimitivePageStatsCollector.newInstance(dataType));
        if (dataType == DataTypes.BOOLEAN) {
          for (int i = 0; i < dataPage.length; i++) {
            adaptivePage.putData(i, ByteUtil.toBoolean((byte) dataPage[i]));
          }
        } else {
          for (int i = 0; i < dataPage.length; i++) {
            adaptivePage.putData(i, dataPage[i]);
          }
        }
        // encode data using adaptive
        this.encodedColumnPage =
            DefaultEncodingFactory.getInstance().createEncoder(spec, adaptivePage, null)
                .encode(adaptivePage);
        adaptivePage.freeMemory();
        super.compressedDataPage = encodedColumnPage.getEncodedData().array();
        super.pageIndexGenerator = pageIndexGenerator;
      }
    };
  }
}
