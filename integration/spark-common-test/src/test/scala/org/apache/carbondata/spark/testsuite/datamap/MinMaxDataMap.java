package org.apache.carbondata.spark.testsuite.datamap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.UnsafeMemoryDMStore;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.indexstore.row.DataMapRowImpl;
import org.apache.carbondata.core.indexstore.schema.DataMapSchema;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;

public class MinMaxDataMap implements DataMap {

  public static final String NAME = "clustered.btree.minmax";

  private static int KEY_INDEX = 0;

  private static int MIN_VALUES_INDEX = 1;

  private static int MAX_VALUES_INDEX = 2;

  private UnsafeMemoryDMStore unsafeMemoryDMStore;

  private SegmentProperties segmentProperties;

  private int[] columnCardinality;


  @Override public void init(String filePath) throws MemoryException, IOException {
    long startTime = System.currentTimeMillis();
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    List<DataFileFooter> indexInfo = fileFooterConverter.getIndexInfo(filePath);
    for (DataFileFooter fileFooter : indexInfo) {
      List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
      if (segmentProperties == null) {
        columnCardinality = fileFooter.getSegmentInfo().getColumnCardinality();
        segmentProperties = new SegmentProperties(columnInTable, columnCardinality);
        //createSchema(segmentProperties);
      }
      TableBlockInfo blockInfo = fileFooter.getBlockInfo().getTableBlockInfo();
      if (fileFooter.getBlockletList() == null || fileFooter.getBlockletList().size() == 0) {
//        LOGGER
//            .info("Reading carbondata file footer to get blocklet info " + blockInfo.getFilePath());
        fileFooter = CarbonUtil.readMetadatFile(blockInfo);
      }

      loadToUnsafe(fileFooter, segmentProperties, blockInfo.getFilePath());
    }
    if (unsafeMemoryDMStore != null) {
      unsafeMemoryDMStore.finishWriting();
    }
//    LOGGER.info("Time taken to load blocklet datamap from file : " + filePath + "is " +
//        (System.currentTimeMillis() - startTime));

  }

  @Override public List<Blocklet> prune(FilterResolverIntf filterExp) {
    return null;
  }

  @Override public void clear() {

  }

  public void updateMinMaxIndex(String filePath) throws IOException, MemoryException {
    long startTime = System.currentTimeMillis();
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    List<DataFileFooter> indexInfo = fileFooterConverter.getIndexInfo(filePath);
    for (DataFileFooter fileFooter : indexInfo) {
      List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
      if (segmentProperties == null) {
        columnCardinality = fileFooter.getSegmentInfo().getColumnCardinality();
        segmentProperties = new SegmentProperties(columnInTable, columnCardinality);
        createSchema(segmentProperties);
      }
      TableBlockInfo blockInfo = fileFooter.getBlockInfo().getTableBlockInfo();
      if (fileFooter.getBlockletList() == null || fileFooter.getBlockletList().size() == 0) {
//        LOGGER
//            .info("Reading carbondata file footer to get blocklet info " + blockInfo.getFilePath());
        fileFooter = CarbonUtil.readMetadatFile(blockInfo);
      }

      loadToUnsafe(fileFooter, segmentProperties, blockInfo.getFilePath());
    }
    if (unsafeMemoryDMStore != null) {
      unsafeMemoryDMStore.finishWriting();
    }
//    LOGGER.info("Time taken to load blocklet datamap from file : " + filePath + "is " +
//        (System.currentTimeMillis() - startTime));

  }

  private void createSchema(SegmentProperties segmentProperties) throws MemoryException {

    // 1.Schema Contains
    // a. Min
    // b. Max
    // c. Block path
    // d. Blocklet info.
    List<DataMapSchema> indexSchemas = new ArrayList<>();

    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    // do it 2 times, one for min and one for max.
    for (int k = 0; k < 2; k++) {
      DataMapSchema[] mapSchemas = new DataMapSchema[minMaxLen.length];
      for (int i = 0; i < minMaxLen.length; i++) {
        if (minMaxLen[i] <= 0) {
          mapSchemas[i] = new DataMapSchema.VariableDataMapSchema(DataType.BYTE_ARRAY);
        } else {
          mapSchemas[i] = new DataMapSchema.FixedDataMapSchema(DataType.BYTE_ARRAY, minMaxLen[i]);
        }
      }
      DataMapSchema mapSchema = new DataMapSchema.StructDataMapSchema(DataType.STRUCT, mapSchemas);
      indexSchemas.add(mapSchema);
    }
    // for table block path
    indexSchemas.add(new DataMapSchema.VariableDataMapSchema(DataType.BYTE_ARRAY));

    //for blocklet info
    indexSchemas.add(new DataMapSchema.VariableDataMapSchema(DataType.BYTE_ARRAY));

    unsafeMemoryDMStore =
        new UnsafeMemoryDMStore(indexSchemas.toArray(new DataMapSchema[indexSchemas.size()]));
  }

  private void loadToUnsafe(DataFileFooter fileFooter, SegmentProperties segmentProperties,
      String filePath) {
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    List<BlockletInfo> blockletList = fileFooter.getBlockletList();
    DataMapSchema[] schema = unsafeMemoryDMStore.getSchema();
    for (int index = 0; index < blockletList.size(); index++) {
      DataMapRow row = new DataMapRowImpl(schema);
      int ordinal = 0;
      BlockletInfo blockletInfo = blockletList.get(index);

      BlockletMinMaxIndex minMaxIndex = blockletInfo.getBlockletIndex().getMinMaxIndex();
      row.setRow(addMinMax(minMaxLen, schema[ordinal], minMaxIndex.getMinValues()), ordinal);
      ordinal++;
      row.setRow(addMinMax(minMaxLen, schema[ordinal], minMaxIndex.getMaxValues()), ordinal);
      ordinal++;

      // add file path
      byte[] filePathBytes =
          filePath.getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
      row.setByteArray(filePathBytes, ordinal++);

      // add blocklet info
      byte[] serializedData;
      try {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(stream);
        blockletInfo.write(dataOutput);
        serializedData = stream.toByteArray();
        row.setByteArray(serializedData, ordinal);
        unsafeMemoryDMStore.addIndexRowToUnsafe(row);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private DataMapRow addMinMax(int[] minMaxLen, DataMapSchema dataMapSchema, byte[][] minValues) {
    DataMapSchema[] minSchemas =
        ((DataMapSchema.StructDataMapSchema) dataMapSchema).getChildSchemas();
    DataMapRow minRow = new DataMapRowImpl(minSchemas);
    int minOrdinal = 0;
    // min value adding
    for (int i = 0; i < minMaxLen.length; i++) {
      minRow.setByteArray(minValues[i], minOrdinal++);
    }
    return minRow;
  }

}
