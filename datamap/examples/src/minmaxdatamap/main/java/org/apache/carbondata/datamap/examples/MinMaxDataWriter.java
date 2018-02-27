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

package org.apache.carbondata.datamap.examples;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.AbstractDataMapWriter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;

public class MinMaxDataWriter extends AbstractDataMapWriter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TableInfo.class.getName());

  private byte[][] pageLevelMin, pageLevelMax;

  private byte[][] blockletLevelMin, blockletLevelMax;

  private Map<Integer, BlockletMinMax> blockMinMaxMap;

  private String dataWritePath;

  public MinMaxDataWriter(AbsoluteTableIdentifier identifier, Segment segment,
      String dataWritePath) {
    super(identifier, segment, dataWritePath);
    this.identifier = identifier;
    this.segmentId = segment.getSegmentNo();
    this.dataWritePath = dataWritePath;
  }

  @Override public void onBlockStart(String blockId) {
    pageLevelMax = null;
    pageLevelMin = null;
    blockletLevelMax = null;
    blockletLevelMin = null;
    blockMinMaxMap = null;
    blockMinMaxMap = new HashMap<Integer, BlockletMinMax>();
  }

  @Override public void onBlockEnd(String blockId) {
    updateMinMaxIndex(blockId);
  }

  @Override public void onBlockletStart(int blockletId) {
  }

  @Override public void onBlockletEnd(int blockletId) {
    updateBlockletMinMax(blockletId);
  }

  @Override
  public void onPageAdded(int blockletId, int pageId, ColumnPage[] pages) {
    // Calculate Min and Max value within this page.

    // As part of example we are extracting Min Max values Manually. The same can be done from
    // retrieving the page statistics. For e.g.

    // if (pageLevelMin == null && pageLevelMax == null) {
    //    pageLevelMin[1] = CarbonUtil.getValueAsBytes(pages[0].getStatistics().getDataType(),
    //        pages[0].getStatistics().getMin());
    //    pageLevelMax[1] = CarbonUtil.getValueAsBytes(pages[0].getStatistics().getDataType(),
    //        pages[0].getStatistics().getMax());
    //  } else {
    //    if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(pageLevelMin[1], CarbonUtil
    //        .getValueAsBytes(pages[0].getStatistics().getDataType(),
    //            pages[0].getStatistics().getMin())) > 0) {
    //      pageLevelMin[1] = CarbonUtil.getValueAsBytes(pages[0].getStatistics().getDataType(),
    //          pages[0].getStatistics().getMin());
    //    }
    //    if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(pageLevelMax[1], CarbonUtil
    //        .getValueAsBytes(pages[0].getStatistics().getDataType(),
    //            pages[0].getStatistics().getMax())) < 0) {
    //      pageLevelMax[1] = CarbonUtil.getValueAsBytes(pages[0].getStatistics().getDataType(),
    //          pages[0].getStatistics().getMax());
    //    }

    byte[] value = new byte[pages[0].getBytes(0).length - 2];
    if (pageLevelMin == null && pageLevelMax == null) {
      pageLevelMin = new byte[2][];
      pageLevelMax = new byte[2][];

      System.arraycopy(pages[0].getBytes(0), 2, value, 0, value.length);
      pageLevelMin[1] = value;
      pageLevelMax[1] = value;

    } else {
      for (int rowIndex = 0; rowIndex < pages[0].getPageSize(); rowIndex++) {
        System.arraycopy(pages[0].getBytes(rowIndex), 2, value, 0, value.length);
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(pageLevelMin[1], value) > 0) {
          pageLevelMin[1] = value;
        }
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(pageLevelMax[1], value) < 0) {
          pageLevelMax[1] = value;
        }
      }
    }
  }

  private void updateBlockletMinMax(int blockletId) {
    if (blockletLevelMax == null || blockletLevelMin == null) {
      blockletLevelMax = new byte[2][];
      blockletLevelMin = new byte[2][];
      if (pageLevelMax != null || pageLevelMin != null) {
        blockletLevelMin = pageLevelMin;
        blockletLevelMax = pageLevelMax;
      }
    } else {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockletLevelMin[1], pageLevelMin[1]) > 0) {
        blockletLevelMin = pageLevelMin;
      }

      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockletLevelMax[1], pageLevelMax[1]) > 0) {
        blockletLevelMax = pageLevelMax;
      }
    }
    BlockletMinMax blockletMinMax = new BlockletMinMax();
    blockletMinMax.setMax(blockletLevelMax);
    blockletMinMax.setMin(blockletLevelMin);
    blockMinMaxMap.put(blockletId, blockletMinMax);
  }


  public void updateMinMaxIndex(String blockId) {
    constructMinMaxIndex(blockId);
  }



  /**
   * Construct the Min Max Index.
   * @param blockId
   */
  public void constructMinMaxIndex(String blockId) {
    // construct Min and Max values of each Blocklets present inside a block.
    List<MinMaxIndexBlockDetails> tempMinMaxIndexBlockDetails = null;
    tempMinMaxIndexBlockDetails = loadBlockDetails();
    try {
      writeMinMaxIndexFile(tempMinMaxIndexBlockDetails, blockId);
    } catch (IOException ex) {
      LOGGER.info(" Unable to write the file");
    }
  }

  /**
   * loadBlockDetails into the MinMaxIndexBlockDetails class.
   */
  private List<MinMaxIndexBlockDetails> loadBlockDetails() {
    List<MinMaxIndexBlockDetails> minMaxIndexBlockDetails = new ArrayList<MinMaxIndexBlockDetails>();
    MinMaxIndexBlockDetails tmpminMaxIndexBlockDetails = new MinMaxIndexBlockDetails();

    for (int index = 0; index < blockMinMaxMap.size(); index++) {
      tmpminMaxIndexBlockDetails.setMinValues(blockMinMaxMap.get(index).getMin());
      tmpminMaxIndexBlockDetails.setMaxValues(blockMinMaxMap.get(index).getMax());
      tmpminMaxIndexBlockDetails.setBlockletId(index);
      minMaxIndexBlockDetails.add(tmpminMaxIndexBlockDetails);
    }
    return minMaxIndexBlockDetails;
  }

  /**
   * Write the data to a file. This is JSON format file.
   * @param minMaxIndexBlockDetails
   * @param blockId
   * @throws IOException
   */
  public void writeMinMaxIndexFile(List<MinMaxIndexBlockDetails> minMaxIndexBlockDetails,
      String blockId) throws IOException {
    String filePath = dataWritePath +"/" + blockId + ".minmaxindex";
    BufferedWriter brWriter = null;
    DataOutputStream dataOutStream = null;
    try {
      FileFactory.createNewFile(filePath, FileFactory.getFileType(filePath));
      dataOutStream = FileFactory.getDataOutputStream(filePath, FileFactory.getFileType(filePath));
      Gson gsonObjectToWrite = new Gson();
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutStream, "UTF-8"));
      String minmaxIndexData = gsonObjectToWrite.toJson(minMaxIndexBlockDetails);
      brWriter.write(minmaxIndexData);
    } catch (IOException ioe) {
      LOGGER.info("Error in writing minMaxindex file");
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      if (null != dataOutStream) {
        dataOutStream.flush();
      }
      CarbonUtil.closeStreams(brWriter, dataOutStream);
      commitFile(filePath);
    }
  }

  @Override public void finish() throws IOException {

  }
}