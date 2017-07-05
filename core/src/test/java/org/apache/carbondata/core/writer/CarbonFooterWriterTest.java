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

package org.apache.carbondata.core.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.page.statistics.MeasurePageStatsVO;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.BlockletInfoColumnar;
import org.apache.carbondata.core.reader.CarbonFooterReader;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;

import junit.framework.TestCase;
import org.apache.carbondata.format.ColumnSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class will test the functionality writing and
 * reading a dictionary and its corresponding metadata file
 */
public class CarbonFooterWriterTest extends TestCase{

  private String filePath;

  @Before public void setUp() throws Exception {
    filePath = "testMeta.fact";
    deleteFile();
    createFile();
  }

  @After public void tearDown() throws Exception {
    deleteFile();
  }

  /**
   * test writing fact metadata.
   */
  @Test public void testWriteFactMetadata() throws IOException {
    deleteFile();
    createFile();
    CarbonFooterWriter writer = new CarbonFooterWriter(filePath);

    List<BlockletInfoColumnar> infoColumnars = getBlockletInfoColumnars();

    int[] cardinalities = new int[] { 2, 4, 5, 7, 9, 10 };
    List<ColumnSchema> columnSchema = Arrays.asList(new ColumnSchema[]{getDimensionColumn("IMEI1"),
						getDimensionColumn("IMEI2"),
						getDimensionColumn("IMEI3"),
						getDimensionColumn("IMEI4"),
						getDimensionColumn("IMEI5"),
						getDimensionColumn("IMEI6")});
    List<org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema> wrapperColumnSchema = Arrays.asList(new org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema[]{getWrapperDimensionColumn("IMEI1"),
    	getWrapperDimensionColumn("IMEI2"),
    	getWrapperDimensionColumn("IMEI3"),
    	getWrapperDimensionColumn("IMEI4"),
    	getWrapperDimensionColumn("IMEI5"),
    	getWrapperDimensionColumn("IMEI6")});
    int[] colCardinality = CarbonUtil.getFormattedCardinality(cardinalities, wrapperColumnSchema);
    SegmentProperties segmentProperties = new SegmentProperties(wrapperColumnSchema, colCardinality);
		writer.writeFooter(CarbonMetadataUtil.convertFileFooter(
				infoColumnars,
				6,
				cardinalities,columnSchema, segmentProperties
				), 0);

    CarbonFooterReader metaDataReader = new CarbonFooterReader(filePath, 0);
    assertTrue(metaDataReader.readFooter() != null);
  }
  
  public static ColumnSchema getDimensionColumn(String columnName) {
	    ColumnSchema dimColumn = new ColumnSchema();
	    dimColumn.setColumnar(true);
	    dimColumn.setColumn_name(columnName);
	    dimColumn.setColumn_id(UUID.randomUUID().toString());
	    dimColumn.setData_type(org.apache.carbondata.format.DataType.STRING);
	    dimColumn.setDimension(true);
	    List<org.apache.carbondata.format.Encoding> encodeList =
		        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
	    encodeList.add(org.apache.carbondata.format.Encoding.DICTIONARY);
	    dimColumn.setEncoders(encodeList);
	    dimColumn.setNum_child(0);
	    return dimColumn;
	  }
  public static org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema getWrapperDimensionColumn(String columnName) {
   org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema dimColumn = new org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema();
   dimColumn.setColumnar(true);
   dimColumn.setColumnName(columnName);
   dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
   dimColumn.setDataType(DataType.STRING);
   dimColumn.setDimensionColumn(true);
   List<Encoding> encodeList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
   encodeList.add(Encoding.DICTIONARY);
   dimColumn.setEncodingList(encodeList);
   dimColumn.setNumberOfChild(0);
    return dimColumn;
 }

  /**
   * test writing fact metadata.
   */
  @Test public void testReadFactMetadata() throws IOException {
    deleteFile();
    createFile();
    CarbonFooterWriter writer = new CarbonFooterWriter(filePath);
    List<BlockletInfoColumnar> infoColumnars = getBlockletInfoColumnars();
    int[] cardinalities = new int[] { 2, 4, 5, 7, 9, 10};
    List<ColumnSchema> columnSchema = Arrays.asList(new ColumnSchema[]{getDimensionColumn("IMEI1"),
						getDimensionColumn("IMEI2"),
						getDimensionColumn("IMEI3"),
						getDimensionColumn("IMEI4"),
						getDimensionColumn("IMEI5"),
						getDimensionColumn("IMEI6")});
    List<org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema> wrapperColumnSchema = Arrays.asList(new org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema[]{getWrapperDimensionColumn("IMEI1"),
    	getWrapperDimensionColumn("IMEI2"),
    	getWrapperDimensionColumn("IMEI3"),
    	getWrapperDimensionColumn("IMEI4"),
    	getWrapperDimensionColumn("IMEI5"),
    	getWrapperDimensionColumn("IMEI6")});
    int[] colCardinality = CarbonUtil.getFormattedCardinality(cardinalities, wrapperColumnSchema);
    SegmentProperties segmentProperties = new SegmentProperties(wrapperColumnSchema, cardinalities);
    writer.writeFooter(CarbonMetadataUtil
        .convertFileFooter(infoColumnars, 6, colCardinality,
        		columnSchema,segmentProperties), 0);

    CarbonFooterReader metaDataReader = new CarbonFooterReader(filePath, 0);
    List<BlockletInfoColumnar> nodeInfoColumnars =
        CarbonMetadataUtil.convertBlockletInfo(metaDataReader.readFooter());

    assertTrue(nodeInfoColumnars.size() == infoColumnars.size());
  }

  private List<BlockletInfoColumnar> getBlockletInfoColumnars() {
    BlockletInfoColumnar infoColumnar = new BlockletInfoColumnar();
    infoColumnar.setStartKey(new byte[] { 1, 2, 3 });
    infoColumnar.setEndKey(new byte[] { 8, 9, 10 });
    infoColumnar.setKeyLengths(new int[] { 1, 2, 3, 4 });
    infoColumnar.setKeyOffSets(new long[] { 22, 44, 55, 77 });
    infoColumnar.setIsSortedKeyColumn(new boolean[] { false, true, false, true });
    infoColumnar.setColumnMaxData(
        new byte[][] { new byte[] { 1, 2 }, new byte[] { 3, 4 }, new byte[] { 4, 5 },
            new byte[] { 5, 6 } });
    infoColumnar.setColumnMinData(
        new byte[][] { new byte[] { 1, 2 }, new byte[] { 3, 4 }, new byte[] { 4, 5 },
            new byte[] { 5, 6 } });
    infoColumnar.setKeyBlockIndexLength(new int[] { 4, 7 });
    infoColumnar.setKeyBlockIndexOffSets(new long[] { 55, 88 });
    infoColumnar.setDataIndexMapLength(new int[] { 2, 6, 7, 8 });
    infoColumnar.setDataIndexMapOffsets(new long[] { 77, 88, 99, 111 });
    infoColumnar.setMeasureLength(new int[] { 6, 7 });
    infoColumnar.setMeasureOffset(new long[] { 33, 99 });
    infoColumnar.setAggKeyBlock(new boolean[] { true, true, true, true });
    infoColumnar.setColGrpBlocks(new boolean[] { false, false, false, false });
    infoColumnar.setMeasureNullValueIndex(new BitSet[] {new BitSet(),new BitSet()});

    ValueEncoderMeta[] metas = new ValueEncoderMeta[2];
    metas[0] = new ValueEncoderMeta();
    metas[0].setMinValue(0);
    metas[0].setMaxValue(44d);
    metas[0].setUniqueValue(0d);
    metas[0].setDecimal(0);
    metas[0].setType(CarbonCommonConstants.DOUBLE_MEASURE);
    metas[0].setDataTypeSelected((byte)0);
    metas[1] = new ValueEncoderMeta();
    metas[1].setMinValue(0);
    metas[1].setMaxValue(55d);
    metas[1].setUniqueValue(0d);
    metas[1].setDecimal(0);
    metas[1].setType(CarbonCommonConstants.DOUBLE_MEASURE);
    metas[1].setDataTypeSelected((byte)0);

    MeasurePageStatsVO stats = MeasurePageStatsVO.build(metas);
    infoColumnar.setStats(stats);
    List<BlockletInfoColumnar> infoColumnars = new ArrayList<BlockletInfoColumnar>();
    infoColumnars.add(infoColumnar);
    return infoColumnars;
  }

  /**
   * this method will delete file
   */
  private void deleteFile() {
    FileFactory.FileType fileType = FileFactory.getFileType(this.filePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(this.filePath, fileType);
    carbonFile.delete();
  }

  private void createFile() {
    FileFactory.FileType fileType = FileFactory.getFileType(this.filePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(this.filePath, fileType);
    carbonFile.createNewFile();
  }

}
