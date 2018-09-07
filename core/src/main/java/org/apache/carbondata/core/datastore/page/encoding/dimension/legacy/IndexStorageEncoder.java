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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.SortState;

public abstract class IndexStorageEncoder extends ColumnPageEncoder {
  PageIndexGenerator pageIndexGenerator;
  byte[] compressedDataPage;

  abstract void encodeIndexStorage(ColumnPage inputPage);

  @Override
  protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
    encodeIndexStorage(input);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    out.write(compressedDataPage);
    if (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) {
      out.writeInt(pageIndexGenerator.getRowIdPageLengthInBytes());
      short[] rowIdPage = pageIndexGenerator.getRowIdPage();
      for (short rowId : rowIdPage) {
        out.writeShort(rowId);
      }
      if (pageIndexGenerator.getRowIdRlePageLengthInBytes() > 0) {
        short[] rowIdRlePage = pageIndexGenerator.getRowIdRlePage();
        for (short rowIdRle : rowIdRlePage) {
          out.writeShort(rowIdRle);
        }
      }
    }
    if (pageIndexGenerator.getDataRlePageLengthInBytes() > 0) {
      short[] dataRlePage = pageIndexGenerator.getDataRlePage();
      for (short dataRle : dataRlePage) {
        out.writeShort(dataRle);
      }
    }
    return stream.toByteArray();
  }

  @Override
  protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
    return null;
  }

  @Override
  protected void fillLegacyFields(DataChunk2 dataChunk)
      throws IOException {
    SortState sort = (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) ?
        SortState.SORT_EXPLICIT : SortState.SORT_NATIVE;
    dataChunk.setSort_state(sort);
    if (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) {
      int rowIdPageLength = CarbonCommonConstants.INT_SIZE_IN_BYTE +
          pageIndexGenerator.getRowIdPageLengthInBytes() +
          pageIndexGenerator.getRowIdRlePageLengthInBytes();
      dataChunk.setRowid_page_length(rowIdPageLength);
    }
    if (pageIndexGenerator.getDataRlePageLengthInBytes() > 0) {
      dataChunk.setRle_page_length(pageIndexGenerator.getDataRlePageLengthInBytes());
    }
    dataChunk.setData_page_length(compressedDataPage.length);
  }
}