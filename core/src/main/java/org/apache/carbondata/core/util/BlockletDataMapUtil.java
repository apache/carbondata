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

package org.apache.carbondata.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.filesystem.AbstractDFSCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.S3CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

public class BlockletDataMapUtil {

  private static final Logger LOG =
      LogServiceFactory.getLogService(BlockletDataMapUtil.class.getName());

  public static Map<String, BlockMetaInfo> getBlockMetaInfoMap(
      TableBlockIndexUniqueIdentifierWrapper identifierWrapper,
      SegmentIndexFileStore indexFileStore, Set<String> filesRead,
      Map<String, BlockMetaInfo> fileNameToMetaInfoMapping, List<DataFileFooter> indexInfos)
      throws IOException {
    boolean isTransactionalTable = true;
    TableBlockIndexUniqueIdentifier identifier =
        identifierWrapper.getTableBlockIndexUniqueIdentifier();
    List<ColumnSchema> tableColumnList = null;
    if (identifier.getMergeIndexFileName() != null
        && indexFileStore.getFileData(identifier.getIndexFileName()) == null) {
      CarbonFile indexMergeFile = FileFactory.getCarbonFile(
          identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
              .getMergeIndexFileName(), identifierWrapper.getConfiguration());
      if (indexMergeFile.exists() && !filesRead.contains(indexMergeFile.getPath())) {
        indexFileStore.readAllIIndexOfSegment(new CarbonFile[] { indexMergeFile });
        filesRead.add(indexMergeFile.getPath());
      }
    }
    if (indexFileStore.getFileData(identifier.getIndexFileName()) == null) {
      indexFileStore.readAllIIndexOfSegment(new CarbonFile[] { FileFactory.getCarbonFile(
          identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
              .getIndexFileName(), identifierWrapper.getConfiguration()) });
    }
    Map<String, BlockMetaInfo> blockMetaInfoMap = new HashMap<>();
    CarbonTable carbonTable = identifierWrapper.getCarbonTable();
    if (carbonTable != null) {
      isTransactionalTable = carbonTable.getTableInfo().isTransactionalTable();
      tableColumnList =
          carbonTable.getTableInfo().getFactTable().getListOfColumns();
    }
    DataFileFooterConverter fileFooterConverter =
        new DataFileFooterConverter(identifierWrapper.getConfiguration());
    List<DataFileFooter> indexInfo = fileFooterConverter.getIndexInfo(
        identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
            .getIndexFileName(), indexFileStore.getFileData(identifier.getIndexFileName()),
        isTransactionalTable);
    indexInfos.addAll(indexInfo);
    for (DataFileFooter footer : indexInfo) {
      if ((!isTransactionalTable) && (tableColumnList.size() != 0) &&
          !isSameColumnAndDifferentDatatypeInSchema(footer.getColumnInTable(), tableColumnList)) {
        LOG.error("Datatype of the common columns present in " + identifier.getIndexFileName()
            + " doesn't match with the column's datatype in table schema");
        throw new IOException("All common columns present in the files doesn't have same datatype. "
            + "Unsupported operation on nonTransactional table. Check logs.");
      }
      if ((tableColumnList != null) && (tableColumnList.size() == 0)) {
        // Carbon reader have used dummy columnSchema. Update it with inferred schema now
        carbonTable.getTableInfo().getFactTable().setListOfColumns(footer.getColumnInTable());
        CarbonTable.updateTableByTableInfo(carbonTable, carbonTable.getTableInfo());
      }
      String blockPath = footer.getBlockInfo().getTableBlockInfo().getFilePath();
      if (null == blockMetaInfoMap.get(blockPath)) {
        BlockMetaInfo blockMetaInfo = createBlockMetaInfo(
            fileNameToMetaInfoMapping, footer.getBlockInfo().getTableBlockInfo());
        // if blockMetaInfo is null that means the file has been deleted from the file system.
        // This can happen in case IUD scenarios where after deleting or updating the data the
        // complete block is deleted but the entry still exists in index or merge index file
        if (null != blockMetaInfo) {
          blockMetaInfoMap.put(blockPath, blockMetaInfo);
        }
      }
    }
    return blockMetaInfoMap;
  }

  /**
   * This method will create file name to block Meta Info Mapping. This method will reduce the
   * number of namenode calls and using this method one namenode will fetch 1000 entries
   *
   * @param segmentFilePath
   * @return
   * @throws IOException
   */
  public static Map<String, BlockMetaInfo> createCarbonDataFileBlockMetaInfoMapping(
      String segmentFilePath, Configuration configuration) throws IOException {
    Map<String, BlockMetaInfo> fileNameToMetaInfoMapping = new TreeMap();
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentFilePath, configuration);
    if (carbonFile instanceof AbstractDFSCarbonFile && !(carbonFile instanceof S3CarbonFile)) {
      PathFilter pathFilter = new PathFilter() {
        @Override public boolean accept(Path path) {
          return CarbonTablePath.isCarbonDataFile(path.getName());
        }
      };
      CarbonFile[] carbonFiles = carbonFile.locationAwareListFiles(pathFilter);
      for (CarbonFile file : carbonFiles) {
        String[] location = file.getLocations();
        long len = file.getSize();
        BlockMetaInfo blockMetaInfo = new BlockMetaInfo(location, len);
        fileNameToMetaInfoMapping.put(file.getPath(), blockMetaInfo);
      }
    }
    return fileNameToMetaInfoMapping;
  }

  private static BlockMetaInfo createBlockMetaInfo(
      Map<String, BlockMetaInfo> fileNameToMetaInfoMapping, TableBlockInfo blockInfo)
      throws IOException {
    String carbonDataFile = blockInfo.getFilePath();
    FileFactory.FileType fileType = FileFactory.getFileType(carbonDataFile);
    switch (fileType) {
      case S3:
      case LOCAL:
        // consider backward compatibility
        // when the file size in blockInfo is not zero, use this file size in blockInfo.
        if (blockInfo.getFileSize() != 0) {
          return new BlockMetaInfo(new String[] { "localhost" }, blockInfo.getFileSize());
        }
        // when the file size in blockInfo is zero, get the size of this file.
        if (!FileFactory.isFileExist(carbonDataFile)) {
          return null;
        }
        CarbonFile carbonFile = FileFactory.getCarbonFile(carbonDataFile, fileType);
        return new BlockMetaInfo(new String[] { "localhost" }, carbonFile.getSize());
      default:
        return fileNameToMetaInfoMapping.get(carbonDataFile);
    }
  }

  public static Set<TableBlockIndexUniqueIdentifier> getTableBlockUniqueIdentifiers(Segment segment)
      throws IOException {
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers = new HashSet<>();
    Map<String, String> indexFiles = segment.getCommittedIndexFile();
    for (Map.Entry<String, String> indexFileEntry : indexFiles.entrySet()) {
      Path indexFile = new Path(indexFileEntry.getKey());
      tableBlockIndexUniqueIdentifiers.add(
          new TableBlockIndexUniqueIdentifier(indexFile.getParent().toString(), indexFile.getName(),
              indexFileEntry.getValue(), segment.getSegmentNo()));
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * This method will filter out the TableBlockIndexUniqueIdentifier belongs to that distributable
   *
   * @param tableBlockIndexUniqueIdentifiers
   * @param distributable
   * @return
   */
  public static TableBlockIndexUniqueIdentifier filterIdentifiersBasedOnDistributable(
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers,
      BlockletDataMapDistributable distributable) {
    TableBlockIndexUniqueIdentifier validIdentifier = null;
    String fileName = CarbonTablePath.DataFileUtil.getFileName(distributable.getFilePath());
    for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier :
        tableBlockIndexUniqueIdentifiers) {
      if (fileName.equals(tableBlockIndexUniqueIdentifier.getIndexFileName())) {
        validIdentifier = tableBlockIndexUniqueIdentifier;
        break;
      }
    }
    return validIdentifier;
  }

  /**
   * This method will the index files tableBlockIndexUniqueIdentifiers of a merge index file
   *
   * @param identifier
   * @param segmentIndexFileStore
   * @return
   * @throws IOException
   */
  public static List<TableBlockIndexUniqueIdentifier> getIndexFileIdentifiersFromMergeFile(
      TableBlockIndexUniqueIdentifier identifier, SegmentIndexFileStore segmentIndexFileStore)
      throws IOException {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers = new ArrayList<>();
    String mergeFilePath =
        identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
            .getIndexFileName();
    segmentIndexFileStore.readMergeFile(mergeFilePath);
    List<String> indexFiles =
        segmentIndexFileStore.getCarbonMergeFileToIndexFilesMap().get(mergeFilePath);
    for (String indexFile : indexFiles) {
      tableBlockIndexUniqueIdentifiers.add(
          new TableBlockIndexUniqueIdentifier(identifier.getIndexFilePath(), indexFile,
              identifier.getIndexFileName(), identifier.getSegmentId()));
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * Method to check if CACHE_LEVEL is set to BLOCK or BLOCKLET
   */
  public static boolean isCacheLevelBlock(CarbonTable carbonTable) {
    String cacheLevel = carbonTable.getTableInfo().getFactTable().getTableProperties()
        .get(CarbonCommonConstants.CACHE_LEVEL);
    if (BlockletDataMapFactory.CACHE_LEVEL_BLOCKLET.equals(cacheLevel)) {
      return false;
    }
    return true;
  }

  /**
   * This method validates whether the schema present in index and table contains the same column
   * name but with different dataType.
   */
  public static boolean isSameColumnAndDifferentDatatypeInSchema(
      List<ColumnSchema> indexFileColumnList, List<ColumnSchema> tableColumnList)
      throws IOException {
    for (int i = 0; i < tableColumnList.size(); i++) {
      for (int j = 0; j < indexFileColumnList.size(); j++) {
        if (indexFileColumnList.get(j).getColumnName()
            .equalsIgnoreCase(tableColumnList.get(i).getColumnName()) && !indexFileColumnList.get(j)
            .getDataType().getName()
            .equalsIgnoreCase(tableColumnList.get(i).getDataType().getName())) {
          if ("varchar".equalsIgnoreCase(indexFileColumnList.get(j).getDataType().getName()) &&
              "string".equalsIgnoreCase(tableColumnList.get(i).getDataType().getName())) {
            throw new IOException("Datatype of the Column "
                + indexFileColumnList.get(j).getDataType().getName()
                + " present in index file, is varchar and not same as datatype of the column " +
                "with same name present in table, " +
                "because carbon convert varchar of carbon to string of spark, " +
                "please set long_string_columns for varchar column: "
                + tableColumnList.get(i).getColumnName());
          }
          LOG.error("Datatype of the Column " + indexFileColumnList.get(j).getColumnName()
              + " present in index file, is not same as datatype of the column with same name"
              + "present in table");
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Fill the measures min values with minimum , this is needed for backward version compatability
   * as older versions don't store min values for measures
   */
  public static byte[][] updateMinValues(SegmentProperties segmentProperties, byte[][] minValues) {
    byte[][] updatedValues = minValues;
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    if (minValues.length < minMaxLen.length) {
      updatedValues = new byte[minMaxLen.length][];
      System.arraycopy(minValues, 0, updatedValues, 0, minValues.length);
      List<CarbonMeasure> measures = segmentProperties.getMeasures();
      ByteBuffer buffer = ByteBuffer.allocate(8);
      for (int i = 0; i < measures.size(); i++) {
        buffer.rewind();
        DataType dataType = measures.get(i).getDataType();
        if (dataType == DataTypes.BYTE) {
          buffer.putLong(Byte.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.SHORT) {
          buffer.putLong(Short.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.INT) {
          buffer.putLong(Integer.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.LONG) {
          buffer.putLong(Long.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        } else if (DataTypes.isDecimal(dataType)) {
          updatedValues[minValues.length + i] =
              DataTypeUtil.bigDecimalToByte(BigDecimal.valueOf(Long.MIN_VALUE));
        } else {
          buffer.putDouble(Double.MIN_VALUE);
          updatedValues[minValues.length + i] = buffer.array().clone();
        }
      }
    }
    return updatedValues;
  }

  /**
   * Fill the measures max values with maximum , this is needed for backward version compatability
   * as older versions don't store max values for measures
   */
  public static byte[][] updateMaxValues(SegmentProperties segmentProperties, byte[][] maxValues) {
    byte[][] updatedValues = maxValues;
    int[] minMaxLen = segmentProperties.getColumnsValueSize();
    if (maxValues.length < minMaxLen.length) {
      updatedValues = new byte[minMaxLen.length][];
      System.arraycopy(maxValues, 0, updatedValues, 0, maxValues.length);
      List<CarbonMeasure> measures = segmentProperties.getMeasures();
      ByteBuffer buffer = ByteBuffer.allocate(8);
      for (int i = 0; i < measures.size(); i++) {
        buffer.rewind();
        DataType dataType = measures.get(i).getDataType();
        if (dataType == DataTypes.BYTE) {
          buffer.putLong(Byte.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.SHORT) {
          buffer.putLong(Short.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.INT) {
          buffer.putLong(Integer.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (dataType == DataTypes.LONG) {
          buffer.putLong(Long.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        } else if (DataTypes.isDecimal(dataType)) {
          updatedValues[maxValues.length + i] =
              DataTypeUtil.bigDecimalToByte(BigDecimal.valueOf(Long.MAX_VALUE));
        } else {
          buffer.putDouble(Double.MAX_VALUE);
          updatedValues[maxValues.length + i] = buffer.array().clone();
        }
      }
    }
    return updatedValues;
  }

  /**
   * Convert schema to binary
   */
  public static byte[] convertSchemaToBinary(List<ColumnSchema> columnSchemas) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(stream);
    dataOutput.writeShort(columnSchemas.size());
    for (ColumnSchema columnSchema : columnSchemas) {
      if (columnSchema.getColumnReferenceId() == null) {
        columnSchema.setColumnReferenceId(columnSchema.getColumnUniqueId());
      }
      columnSchema.write(dataOutput);
    }
    byte[] byteArray = stream.toByteArray();
    // Compress to reduce the size of schema
    return CompressorFactory.NativeSupportedCompressor.SNAPPY.getCompressor().compressByte(
        byteArray);
  }

  /**
   * Read column schema from binary
   *
   * @param schemaArray
   * @throws IOException
   */
  public static List<ColumnSchema> readColumnSchema(byte[] schemaArray) throws IOException {
    // uncompress it.
    schemaArray = CompressorFactory.NativeSupportedCompressor.SNAPPY.getCompressor().unCompressByte(
        schemaArray);
    ByteArrayInputStream schemaStream = new ByteArrayInputStream(schemaArray);
    DataInput schemaInput = new DataInputStream(schemaStream);
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    int size = schemaInput.readShort();
    for (int i = 0; i < size; i++) {
      ColumnSchema columnSchema = new ColumnSchema();
      columnSchema.readFields(schemaInput);
      columnSchemas.add(columnSchema);
    }
    return columnSchemas;
  }

  /**
   * Method to get the min/max values for columns to be cached
   *
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @param minMaxValuesForAllColumns
   * @return
   */
  public static byte[][] getMinMaxForColumnsToBeCached(SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns, byte[][] minMaxValuesForAllColumns) {
    byte[][] minMaxValuesForColumnsToBeCached = minMaxValuesForAllColumns;
    if (null != minMaxCacheColumns) {
      minMaxValuesForColumnsToBeCached = new byte[minMaxCacheColumns.size()][];
      int counter = 0;
      for (CarbonColumn column : minMaxCacheColumns) {
        minMaxValuesForColumnsToBeCached[counter++] =
            minMaxValuesForAllColumns[getColumnOrdinal(segmentProperties, column)];
      }
    }
    return minMaxValuesForColumnsToBeCached;
  }

  /**
   * Method to get the flag values for columns to be cached
   *
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @param minMaxFlag
   * @return
   */
  public static boolean[] getMinMaxFlagValuesForColumnsToBeCached(
      SegmentProperties segmentProperties, List<CarbonColumn> minMaxCacheColumns,
      boolean[] minMaxFlag) {
    boolean[] minMaxFlagValuesForColumnsToBeCached = minMaxFlag;
    if (null != minMaxCacheColumns) {
      minMaxFlagValuesForColumnsToBeCached = new boolean[minMaxCacheColumns.size()];
      int counter = 0;
      for (CarbonColumn column : minMaxCacheColumns) {
        minMaxFlagValuesForColumnsToBeCached[counter++] =
            minMaxFlag[getColumnOrdinal(segmentProperties, column)];
      }
    }
    return minMaxFlagValuesForColumnsToBeCached;
  }

  /**
   * compute the column ordinal as per data is stored
   *
   * @param segmentProperties
   * @param column
   * @return
   */
  public static int getColumnOrdinal(SegmentProperties segmentProperties, CarbonColumn column) {
    if (column.isMeasure()) {
      // as measures are stored at the end after all dimensions and complex dimensions hence add
      // the last dimension ordinal to measure ordinal. Segment properties will store min max
      // length in one array on the order normal dimension, complex dimension and then measure
      return segmentProperties.getLastDimensionColOrdinal() + column.getOrdinal();
    } else {
      return column.getOrdinal();
    }
  }

  /**
   * Method to check whether to serialize min/max values to executor. Returns true if
   * filter column min/max is not cached in driver
   *
   * @param filterResolverTree
   * @param minMaxCacheColumns
   * @return
   */
  public static boolean useMinMaxForBlockletPruning(FilterResolverIntf filterResolverTree,
      List<CarbonColumn> minMaxCacheColumns) {
    boolean serializeMinMax = false;
    if (null != minMaxCacheColumns) {
      Set<CarbonDimension> filterDimensions = new HashSet<>();
      Set<CarbonMeasure> filterMeasures = new HashSet<>();
      QueryUtil
          .getAllFilterDimensionsAndMeasures(filterResolverTree, filterDimensions, filterMeasures);
      // set flag to true if columns cached size is lesser than filter columns
      if (minMaxCacheColumns.size() < (filterDimensions.size() + filterMeasures.size())) {
        serializeMinMax = true;
      } else {
        // check if all the filter dimensions are cached
        for (CarbonDimension filterDimension : filterDimensions) {
          // complex dimensions are not allwed to be specified in COLUMN_META_CACHE property, so
          // cannot validate for complex columns
          if (filterDimension.isComplex()) {
            continue;
          }
          if (!filterColumnExistsInMinMaxColumnList(minMaxCacheColumns, filterDimension)) {
            serializeMinMax = true;
            break;
          }
        }
        // check if all the filter measures are cached only if all filter dimensions are cached
        if (!serializeMinMax) {
          for (CarbonMeasure filterMeasure : filterMeasures) {
            if (!filterColumnExistsInMinMaxColumnList(minMaxCacheColumns, filterMeasure)) {
              serializeMinMax = true;
              break;
            }
          }
        }
      }
    }
    return serializeMinMax;
  }

  /**
   * Method to check for filter column in min/max cache columns list
   *
   * @param minMaxCacheColumns
   * @param filterColumn
   * @return
   */
  private static boolean filterColumnExistsInMinMaxColumnList(List<CarbonColumn> minMaxCacheColumns,
      CarbonColumn filterColumn) {
    for (CarbonColumn column : minMaxCacheColumns) {
      if (filterColumn.getColumnId().equalsIgnoreCase(column.getColumnId())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Method to update the min max flag. For CACHE_LEVEL=BLOCK, for any column if min max is not
   * written in any of the blocklet then for that column the flag will be false for the
   * complete block
   *
   * @param minMaxIndex
   * @param minMaxFlag
   */
  public static void updateMinMaxFlag(BlockletMinMaxIndex minMaxIndex, boolean[] minMaxFlag) {
    boolean[] isMinMaxSet = minMaxIndex.getIsMinMaxSet();
    if (null != isMinMaxSet) {
      for (int i = 0; i < minMaxFlag.length; i++) {
        if (!isMinMaxSet[i]) {
          minMaxFlag[i] = isMinMaxSet[i];
        }
      }
    }
  }
}