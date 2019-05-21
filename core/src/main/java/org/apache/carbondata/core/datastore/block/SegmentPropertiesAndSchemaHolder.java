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
package org.apache.carbondata.core.datastore.block;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.indexstore.schema.SchemaGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.log4j.Logger;

/**
 * Singleton class which will help in creating the segment properties
 */
public class SegmentPropertiesAndSchemaHolder {

  /**
   * Logger
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SegmentPropertiesAndSchemaHolder.class.getName());
  /**
   * SegmentPropertiesAndSchemaHolder instance
   */
  private static final SegmentPropertiesAndSchemaHolder INSTANCE =
      new SegmentPropertiesAndSchemaHolder();
  /**
   * object level lock
   */
  private static final Object lock = new Object();
  /**
   * counter for maintaining the index of segmentProperties
   */
  private static final AtomicInteger segmentPropertiesIndexCounter = new AtomicInteger(0);
  /**
   * holds segmentPropertiesWrapper to segment ID and segmentProperties Index wrapper mapping.
   * Will be used while invaliding a segment and drop table
   */
  private Map<SegmentPropertiesWrapper, SegmentIdAndSegmentPropertiesIndexWrapper>
      segmentPropWrapperToSegmentSetMap = new ConcurrentHashMap<>();
  /**
   * reverse mapping for segmentProperties index to segmentPropertiesWrapper
   */
  private static Map<Integer, SegmentPropertiesWrapper> indexToSegmentPropertiesWrapperMapping =
      new ConcurrentHashMap<>();
  /**
   * Map to be used for table level locking while populating segmentProperties
   */
  private Map<String, Object> absoluteTableIdentifierByteMap = new ConcurrentHashMap<>();

  /**
   * private constructor for singleton instance
   */
  private SegmentPropertiesAndSchemaHolder() {

  }

  public static SegmentPropertiesAndSchemaHolder getInstance() {
    return INSTANCE;
  }

  /**
   * Method to add the segment properties and avoid construction of new segment properties until
   * the schema is not modified
   *
   * @param carbonTable
   * @param columnsInTable
   * @param columnCardinality
   * @param segmentId
   */
  public int addSegmentProperties(CarbonTable carbonTable,
      List<ColumnSchema> columnsInTable, int[] columnCardinality, String segmentId) {
    SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper segmentPropertiesWrapper =
        new SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper(carbonTable,
            columnsInTable, columnCardinality);
    SegmentIdAndSegmentPropertiesIndexWrapper segmentIdSetAndIndexWrapper =
        this.segmentPropWrapperToSegmentSetMap.get(segmentPropertiesWrapper);
    if (null == segmentIdSetAndIndexWrapper) {
      synchronized (getOrCreateTableLock(carbonTable.getAbsoluteTableIdentifier())) {
        segmentIdSetAndIndexWrapper =
            this.segmentPropWrapperToSegmentSetMap.get(segmentPropertiesWrapper);
        if (null == segmentIdSetAndIndexWrapper) {
          // create new segmentProperties
          segmentPropertiesWrapper.initSegmentProperties();
          segmentPropertiesWrapper.addMinMaxColumns(carbonTable);
          int segmentPropertiesIndex = segmentPropertiesIndexCounter.incrementAndGet();
          indexToSegmentPropertiesWrapperMapping
              .put(segmentPropertiesIndex, segmentPropertiesWrapper);
          LOGGER.info("Constructing new SegmentProperties for table: " + carbonTable
              .getCarbonTableIdentifier().getTableUniqueName()
              + ". Current size of segment properties" + " holder list is: "
              + indexToSegmentPropertiesWrapperMapping.size());
          // populate the SegmentIdAndSegmentPropertiesIndexWrapper to maintain the set of segments
          // having same SegmentPropertiesWrapper instance this will used to decide during the
          // tblSegmentsProperties map clean-up for the invalid segments
          segmentIdSetAndIndexWrapper =
              new SegmentIdAndSegmentPropertiesIndexWrapper(segmentId, segmentPropertiesIndex);
          segmentPropWrapperToSegmentSetMap
              .put(segmentPropertiesWrapper, segmentIdSetAndIndexWrapper);
        }
      }
    } else {
      synchronized (getOrCreateTableLock(carbonTable.getAbsoluteTableIdentifier())) {
        segmentIdSetAndIndexWrapper.addSegmentId(segmentId);
        indexToSegmentPropertiesWrapperMapping
            .get(segmentIdSetAndIndexWrapper.getSegmentPropertiesIndex())
            .addMinMaxColumns(carbonTable);
      }
    }
    return segmentIdSetAndIndexWrapper.getSegmentPropertiesIndex();
  }

  /**
   * Method to create table Level lock
   *
   * @param absoluteTableIdentifier
   * @return
   */
  private Object getOrCreateTableLock(AbsoluteTableIdentifier absoluteTableIdentifier) {
    Object tableLock = absoluteTableIdentifierByteMap
        .get(absoluteTableIdentifier.getCarbonTableIdentifier().getTableUniqueName());
    if (null == tableLock) {
      synchronized (lock) {
        tableLock = absoluteTableIdentifierByteMap
            .get(absoluteTableIdentifier.getCarbonTableIdentifier().getTableUniqueName());
        if (null == tableLock) {
          tableLock = new Object();
          absoluteTableIdentifierByteMap
              .put(absoluteTableIdentifier.getCarbonTableIdentifier().getTableUniqueName(),
                  tableLock);
        }
      }
    }
    return tableLock;
  }

  /**
   * Method to get the segment properties from given index
   *
   * @param segmentPropertiesIndex
   * @return
   */
  public SegmentProperties getSegmentProperties(int segmentPropertiesIndex) {
    SegmentPropertiesWrapper segmentPropertiesWrapper =
        getSegmentPropertiesWrapper(segmentPropertiesIndex);
    if (null != segmentPropertiesWrapper) {
      return segmentPropertiesWrapper.getSegmentProperties();
    }
    return null;
  }

  /**
   * Method to get the segment properties from given index
   *
   * @param segmentPropertiesWrapperIndex
   * @return
   */
  public SegmentPropertiesWrapper getSegmentPropertiesWrapper(int segmentPropertiesWrapperIndex) {
    return indexToSegmentPropertiesWrapperMapping.get(segmentPropertiesWrapperIndex);
  }

  /**
   * This method will remove the segment properties from the map on drop table
   *
   * @param absoluteTableIdentifier
   */
  public void invalidate(AbsoluteTableIdentifier absoluteTableIdentifier) {
    List<SegmentPropertiesWrapper> segmentPropertiesWrappersToBeRemoved = new ArrayList<>();
    // remove segmentProperties wrapper entries from the copyOnWriteArrayList
    for (Map.Entry<SegmentPropertiesWrapper, SegmentIdAndSegmentPropertiesIndexWrapper> entry :
          segmentPropWrapperToSegmentSetMap.entrySet()) {
      SegmentPropertiesWrapper segmentPropertiesWrapper = entry.getKey();
      if (segmentPropertiesWrapper.getTableIdentifier().getCarbonTableIdentifier()
          .getTableUniqueName()
          .equals(absoluteTableIdentifier.getCarbonTableIdentifier().getTableUniqueName())) {
        SegmentIdAndSegmentPropertiesIndexWrapper value = entry.getValue();
        // remove from the reverse mapping map
        indexToSegmentPropertiesWrapperMapping.remove(value.getSegmentPropertiesIndex());
        segmentPropertiesWrappersToBeRemoved.add(segmentPropertiesWrapper);
      }
    }
    // remove all the segmentPropertiesWrapper entries from map
    for (SegmentPropertiesWrapper segmentPropertiesWrapper : segmentPropertiesWrappersToBeRemoved) {
      segmentPropWrapperToSegmentSetMap.remove(segmentPropertiesWrapper);
    }
    // remove the table lock
    absoluteTableIdentifierByteMap
        .remove(absoluteTableIdentifier.getCarbonTableIdentifier().getTableUniqueName());
  }

  /**
   * Method to remove the given segment ID
   *
   * @param segmentId
   * @param segmentPropertiesIndex
   * @param clearSegmentWrapperFromMap flag to specify whether to clear segmentPropertiesWrapper
   *                                   from Map if all the segment's using it have become stale
   */
  public void invalidate(String segmentId, int segmentPropertiesIndex,
      boolean clearSegmentWrapperFromMap) {
    SegmentPropertiesWrapper segmentPropertiesWrapper =
        indexToSegmentPropertiesWrapperMapping.get(segmentPropertiesIndex);
    if (null != segmentPropertiesWrapper) {
      SegmentIdAndSegmentPropertiesIndexWrapper segmentIdAndSegmentPropertiesIndexWrapper =
          segmentPropWrapperToSegmentSetMap.get(segmentPropertiesWrapper);
      synchronized (getOrCreateTableLock(segmentPropertiesWrapper.getTableIdentifier())) {
        segmentIdAndSegmentPropertiesIndexWrapper.removeSegmentId(segmentId);
        // if after removal of given SegmentId, the segmentIdSet becomes empty that means this
        // segmentPropertiesWrapper is not getting used at all. In that case this object can be
        // removed from all the holders
        if (clearSegmentWrapperFromMap && segmentIdAndSegmentPropertiesIndexWrapper.segmentIdSet
            .isEmpty()) {
          indexToSegmentPropertiesWrapperMapping.remove(segmentPropertiesIndex);
          segmentPropWrapperToSegmentSetMap.remove(segmentPropertiesWrapper);
        } else if (!clearSegmentWrapperFromMap
            && segmentIdAndSegmentPropertiesIndexWrapper.segmentIdSet.isEmpty()) {
          // min max columns can very when cache is modified. So even though entry is not required
          // to be deleted from map clear the column cache so that it can filled again
          segmentPropertiesWrapper.clear();
          LOGGER.info("cleared min max for segmentProperties at index: " + segmentPropertiesIndex);
        }
      }
    }
  }

  /**
   * add segmentId at given segmentPropertyIndex
   * Note: This method is getting used in extension with other features. Please do not remove
   *
   * @param segmentPropertiesIndex
   * @param segmentId
   */
  public void addSegmentId(int segmentPropertiesIndex, String segmentId) {
    SegmentPropertiesWrapper segmentPropertiesWrapper =
        indexToSegmentPropertiesWrapperMapping.get(segmentPropertiesIndex);
    if (null != segmentPropertiesWrapper) {
      SegmentIdAndSegmentPropertiesIndexWrapper segmentIdAndSegmentPropertiesIndexWrapper =
          segmentPropWrapperToSegmentSetMap.get(segmentPropertiesWrapper);
      synchronized (getOrCreateTableLock(segmentPropertiesWrapper.getTableIdentifier())) {
        segmentIdAndSegmentPropertiesIndexWrapper.addSegmentId(segmentId);
      }
    }
  }

  /**
   * This class wraps tableIdentifier, columnsInTable and columnCardinality as a key to determine
   * whether the SegmentProperties object can be reused.
   */
  public static class SegmentPropertiesWrapper {

    private static final Object taskSchemaLock = new Object();
    private static final Object fileFooterSchemaLock = new Object();
    private static final Object minMaxLock = new Object();

    private List<ColumnSchema> columnsInTable;
    private int[] columnCardinality;
    private SegmentProperties segmentProperties;
    private List<CarbonColumn> minMaxCacheColumns;
    private CarbonTable carbonTable;
    // in case of hybrid store we can have block as well as blocklet schema
    // Scenario: When there is a hybrid store in which few loads are from legacy store which do
    // not contain the blocklet information and hence they will be, by default have cache_level as
    // BLOCK and few loads with latest store which contain the BLOCKLET information and have
    // cache_level BLOCKLET. For these type of scenarios we need to have separate task and footer
    // schemas. For all loads with/without blocklet info there will not be any additional cost
    // of maintaining 2 variables
    private CarbonRowSchema[] taskSummarySchemaForBlock;
    private CarbonRowSchema[] taskSummarySchemaForBlocklet;
    private CarbonRowSchema[] fileFooterEntrySchemaForBlock;
    private CarbonRowSchema[] fileFooterEntrySchemaForBlocklet;

    public SegmentPropertiesWrapper(CarbonTable carbonTable,
        List<ColumnSchema> columnsInTable, int[] columnCardinality) {
      this.carbonTable = carbonTable;
      this.columnsInTable = columnsInTable;
      this.columnCardinality = columnCardinality;
    }

    public void initSegmentProperties() {
      segmentProperties = new SegmentProperties(columnsInTable, columnCardinality);
    }

    public void addMinMaxColumns(CarbonTable carbonTable) {
      if (null == minMaxCacheColumns || minMaxCacheColumns.isEmpty()) {
        minMaxCacheColumns = carbonTable.getMinMaxCacheColumns(segmentProperties);
      }
    }

    /**
     * clear required fields
     */
    public void clear() {
      if (null != minMaxCacheColumns) {
        minMaxCacheColumns = null;
      }

      taskSummarySchemaForBlock = null;
      taskSummarySchemaForBlocklet = null;
      fileFooterEntrySchemaForBlock = null;
      fileFooterEntrySchemaForBlocklet = null;
    }

    @Override public boolean equals(Object obj) {
      if (!(obj instanceof SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper)) {
        return false;
      }
      SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper other =
          (SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper) obj;
      return carbonTable.getAbsoluteTableIdentifier()
          .equals(other.carbonTable.getAbsoluteTableIdentifier()) && checkColumnSchemaEquality(
          columnsInTable, other.columnsInTable) && Arrays
          .equals(columnCardinality, other.columnCardinality);
    }

    private boolean checkColumnSchemaEquality(List<ColumnSchema> obj1, List<ColumnSchema> obj2) {
      if (obj1 == null || obj2 == null || (obj1.size() != obj2.size())) {
        return false;
      }
      boolean exists = true;
      for (int i = 0; i < obj1.size(); i++) {
        if (!obj1.get(i).equalsWithStrictCheck(obj2.get(i))) {
          exists = false;
          break;
        }
      }
      return exists;
    }

    private void sortList(List<ColumnSchema> columnSchemas) {
      Collections.sort(columnSchemas, new Comparator<ColumnSchema>() {
        @Override public int compare(ColumnSchema o1, ColumnSchema o2) {
          return o1.getColumnUniqueId().compareTo(o2.getColumnUniqueId());
        }
      });
    }

    @Override public int hashCode() {
      int allColumnsHashCode = 0;
      // check column order
      StringBuilder builder = new StringBuilder();
      for (ColumnSchema columnSchema: columnsInTable) {
        allColumnsHashCode = allColumnsHashCode + columnSchema.strictHashCode();
        builder.append(columnSchema.getColumnUniqueId()).append(",");
      }
      return carbonTable.getAbsoluteTableIdentifier().hashCode() + allColumnsHashCode + Arrays
          .hashCode(columnCardinality) + builder.toString().hashCode();
    }

    public AbsoluteTableIdentifier getTableIdentifier() {
      return carbonTable.getAbsoluteTableIdentifier();
    }

    public SegmentProperties getSegmentProperties() {
      return segmentProperties;
    }

    public List<ColumnSchema> getColumnsInTable() {
      return columnsInTable;
    }

    public int[] getColumnCardinality() {
      return columnCardinality;
    }

    public CarbonRowSchema[] getTaskSummarySchemaForBlock(boolean storeBlockletCount,
        boolean filePathToBeStored) throws MemoryException {
      if (null == taskSummarySchemaForBlock) {
        synchronized (taskSchemaLock) {
          if (null == taskSummarySchemaForBlock) {
            taskSummarySchemaForBlock = SchemaGenerator
                .createTaskSummarySchema(segmentProperties, getMinMaxCacheColumns(),
                    storeBlockletCount, filePathToBeStored);
          }
        }
      }
      return taskSummarySchemaForBlock;
    }

    public CarbonRowSchema[] getTaskSummarySchemaForBlocklet(boolean storeBlockletCount,
        boolean filePathToBeStored) throws MemoryException {
      if (null == taskSummarySchemaForBlocklet) {
        synchronized (taskSchemaLock) {
          if (null == taskSummarySchemaForBlocklet) {
            taskSummarySchemaForBlocklet = SchemaGenerator
                .createTaskSummarySchema(segmentProperties, getMinMaxCacheColumns(),
                    storeBlockletCount, filePathToBeStored);
          }
        }
      }
      return taskSummarySchemaForBlocklet;
    }

    public CarbonRowSchema[] getBlockFileFooterEntrySchema() {
      if (null == fileFooterEntrySchemaForBlock) {
        synchronized (fileFooterSchemaLock) {
          if (null == fileFooterEntrySchemaForBlock) {
            fileFooterEntrySchemaForBlock =
                SchemaGenerator.createBlockSchema(segmentProperties, getMinMaxCacheColumns());
          }
        }
      }
      return fileFooterEntrySchemaForBlock;
    }

    public CarbonRowSchema[] getBlockletFileFooterEntrySchema() {
      if (null == fileFooterEntrySchemaForBlocklet) {
        synchronized (fileFooterSchemaLock) {
          if (null == fileFooterEntrySchemaForBlocklet) {
            fileFooterEntrySchemaForBlocklet =
                SchemaGenerator.createBlockletSchema(segmentProperties, getMinMaxCacheColumns());
          }
        }
      }
      return fileFooterEntrySchemaForBlocklet;
    }

    public List<CarbonColumn> getMinMaxCacheColumns() {
      if (null == minMaxCacheColumns) {
        synchronized (minMaxLock) {
          if (null == minMaxCacheColumns) {
            addMinMaxColumns(carbonTable);
          }
        }
      }
      return minMaxCacheColumns;
    }

  }

  /**
   * holder for segmentId and segmentPropertiesIndex
   */
  public static class SegmentIdAndSegmentPropertiesIndexWrapper {

    /**
     * set holding all unique segment Id's using the same segmentProperties
     */
    private Set<String> segmentIdSet;
    /**
     * index which maps to segmentPropertiesWrpper Index from where segmentProperties
     * can be retrieved
     */
    private int segmentPropertiesIndex;

    public SegmentIdAndSegmentPropertiesIndexWrapper(String segmentId, int segmentPropertiesIndex) {
      segmentIdSet = new HashSet<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      addSegmentId(segmentId);
      this.segmentPropertiesIndex = segmentPropertiesIndex;
    }

    public void addSegmentId(String segmentId) {
      segmentIdSet.add(segmentId);
    }

    public void removeSegmentId(String segmentId) {
      segmentIdSet.remove(segmentId);
    }

    public int getSegmentPropertiesIndex() {
      return segmentPropertiesIndex;
    }
  }
}
