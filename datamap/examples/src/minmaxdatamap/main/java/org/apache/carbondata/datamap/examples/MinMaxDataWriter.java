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
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MinMaxDataWriter extends DataMapWriter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TableInfo.class.getName());

  private Object[] pageLevelMin, pageLevelMax;

  private Map<Integer, BlockletMinMax> blockMinMaxMap;

  private String dataMapName;
  private int columnCnt;
  private DataType[] dataTypeArray;
  private String carbonIndexFileName;

  /**
   * Since the sequence of indexed columns is defined the same as order in user-created, so
   * map colIdx in user-created to colIdx in MinMaxIndex.
   * Please note that the sequence of min-max values for each column in blocklet-min-max is not
   * the same as indexed columns, so we need to reorder the origin while writing the min-max values
   */
  private Map<Integer, Integer> origin2MinMaxOrdinal = new HashMap<>();

  public MinMaxDataWriter(AbsoluteTableIdentifier identifier, String dataMapName, Segment segment,
      String dataWritePath) {
    super(identifier, segment, dataWritePath);
    this.dataMapName = dataMapName;
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        identifier.getDatabaseName(), identifier.getTableName());
    List<CarbonColumn> cols = carbonTable.getCreateOrderColumn(identifier.getTableName());
    this.columnCnt = cols.size();
    List<CarbonDimension> dimensions = carbonTable.getDimensionByTableName(identifier.getTableName());
    for (int i = 0; i < dimensions.size(); i++) {
      this.origin2MinMaxOrdinal.put(dimensions.get(i).getSchemaOrdinal(),
          dimensions.get(i).getOrdinal());
    }
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(identifier.getTableName());
    for (int i = 0; i < measures.size(); i++) {
      this.origin2MinMaxOrdinal.put(measures.get(i).getSchemaOrdinal(),
          dimensions.size() + measures.get(i).getOrdinal());
    }
  }

  @Override public void onBlockStart(String blockId, long taskId) {
    if (blockMinMaxMap == null) {
      blockMinMaxMap = new HashMap<Integer, BlockletMinMax>();
      carbonIndexFileName = CarbonTablePath.getCarbonIndexFileName(blockId);
    }
  }

  @Override public void onBlockEnd(String blockId) {

  }

  @Override public void onBlockletStart(int blockletId) {
    pageLevelMin = new Object[columnCnt];
    pageLevelMax = new Object[columnCnt];
  }

  @Override public void onBlockletEnd(int blockletId) {
    updateCurrentBlockletMinMax(blockletId);
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

    if (this.dataTypeArray == null) {
      this.dataTypeArray = new DataType[this.columnCnt];
      for (int i = 0; i < this.columnCnt; i++) {
        this.dataTypeArray[i] = pages[i].getDataType();
      }
    }

    // as an example, we don't use page-level min-max generated by native carbondata here, we get
    // the min-max by comparing each row
    for (int rowId = 0; rowId < pages[0].getPageSize(); rowId++) {
      for (int colIdx = 0; colIdx < columnCnt; colIdx++) {
        Object originValue = pages[colIdx].getData(rowId);
        // for string & bytes_array, data is prefixed with length, need to remove it
        if (DataTypes.STRING == pages[colIdx].getDataType()
            || DataTypes.BYTE_ARRAY == pages[colIdx].getDataType()) {
          byte[] valueMin0 = (byte[]) pageLevelMin[colIdx];
          byte[] valueMax0 = (byte[]) pageLevelMax[colIdx];
          byte[] value1 = (byte[]) originValue;
          if (pageLevelMin[colIdx] == null || ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(valueMin0, 0, valueMin0.length, value1, 2, value1.length - 2) > 0) {
            pageLevelMin[colIdx] = new byte[value1.length - 2];
            System.arraycopy(value1, 2, (byte[]) pageLevelMin[colIdx], 0, value1.length - 2);
          }
          if (pageLevelMax[colIdx] == null || ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(valueMax0, 0, valueMax0.length, value1, 2, value1.length - 2) < 0) {
            pageLevelMax[colIdx] = new byte[value1.length - 2];
            System.arraycopy(value1, 2, (byte[]) pageLevelMax[colIdx], 0, value1.length - 2);
          }
        } else if (DataTypes.INT == pages[colIdx].getDataType()) {
          updateMinMax(colIdx, originValue, pages[colIdx].getDataType());
        } else {
          throw new RuntimeException("Not implement yet");
        }
      }
    }
  }

  private void updateMinMax(int colIdx, Object originValue, DataType dataType) {
    if (pageLevelMin[colIdx] == null) {
      pageLevelMin[colIdx] = originValue;
    }
    if (pageLevelMax[colIdx] == null) {
      pageLevelMax[colIdx] = originValue;
    }

    if (DataTypes.SHORT == dataType) {
      if (pageLevelMin[colIdx] == null || (short) pageLevelMin[colIdx] - (short) originValue > 0) {
        pageLevelMin[colIdx] = originValue;
      }
      if (pageLevelMax[colIdx] == null || (short) pageLevelMax[colIdx] - (short) originValue < 0) {
        pageLevelMax[colIdx] = originValue;
      }
    } else if (DataTypes.INT == dataType) {
      if (pageLevelMin[colIdx] == null || (int) pageLevelMin[colIdx] - (int) originValue > 0) {
        pageLevelMin[colIdx] = originValue;
      }
      if (pageLevelMax[colIdx] == null || (int) pageLevelMax[colIdx] - (int) originValue < 0) {
        pageLevelMax[colIdx] = originValue;
      }
    } else if (DataTypes.LONG == dataType) {
      if (pageLevelMin[colIdx] == null || (long) pageLevelMin[colIdx] - (long) originValue > 0) {
        pageLevelMin[colIdx] = originValue;
      }
      if (pageLevelMax[colIdx] == null || (long) pageLevelMax[colIdx] - (long) originValue < 0) {
        pageLevelMax[colIdx] = originValue;
      }
    } else if (DataTypes.DOUBLE == dataType) {
      if (pageLevelMin[colIdx] == null
          || (double) pageLevelMin[colIdx] - (double) originValue > 0) {
        pageLevelMin[colIdx] = originValue;
      }
      if (pageLevelMax[colIdx] == null
          || (double) pageLevelMax[colIdx] - (double) originValue < 0) {
        pageLevelMax[colIdx] = originValue;
      }
    } else {
      // todo:
      throw new RuntimeException("Not implemented yet");
    }
  }

  private void updateCurrentBlockletMinMax(int blockletId) {
    byte[][] max = new byte[this.columnCnt][];
    byte[][] min = new byte[this.columnCnt][];
    for (int i = 0; i < this.columnCnt; i++) {
      int targetColIdx = origin2MinMaxOrdinal.get(i);
      max[targetColIdx] = CarbonUtil.getValueAsBytes(this.dataTypeArray[i], pageLevelMax[i]);
      min[targetColIdx] = CarbonUtil.getValueAsBytes(this.dataTypeArray[i], pageLevelMin[i]);
    }

    BlockletMinMax blockletMinMax = new BlockletMinMax();
    blockletMinMax.setMax(max);
    blockletMinMax.setMin(min);
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

    for (int index = 0; index < blockMinMaxMap.size(); index++) {
      MinMaxIndexBlockDetails tmpminMaxIndexBlockDetails = new MinMaxIndexBlockDetails();
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
    String dataMapDir = genDataMapStorePath(this.writeDirectoryPath, this.dataMapName);
    String filePath = dataMapDir + File.separator + blockId + ".minmaxindex";
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
      throw ioe;
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
    updateMinMaxIndex(carbonIndexFileName);
  }

  /**
   * create and return path that will store the datamap
   *
   * @param dataPath patch to store the carbondata factdata
   * @param dataMapName datamap name
   * @return path to store the datamap
   * @throws IOException
   */
  public static String genDataMapStorePath(String dataPath, String dataMapName)
      throws IOException {
    String dmDir = dataPath + File.separator + dataMapName;
    Path dmPath = FileFactory.getPath(dmDir);
    FileSystem fs = FileFactory.getFileSystem(dmPath);
    if (!fs.exists(dmPath)) {
      fs.mkdirs(dmPath);
    }
    return dmDir;
  }
}