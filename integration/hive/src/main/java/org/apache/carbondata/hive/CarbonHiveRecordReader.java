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

package org.apache.carbondata.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

class CarbonHiveRecordReader extends CarbonRecordReader<ArrayWritable>
    implements org.apache.hadoop.mapred.RecordReader<Void, ArrayWritable> {

  private ArrayWritable valueObj = null;
  private long recordReaderCounter = 0;
  private int[] columnIds;

  public CarbonHiveRecordReader(QueryModel queryModel, CarbonReadSupport<ArrayWritable> readSupport,
      InputSplit inputSplit, JobConf jobConf) throws IOException {
    super(queryModel, readSupport, jobConf);
    initialize(inputSplit, jobConf);
  }

  private void initialize(InputSplit inputSplit, Configuration conf) throws IOException {
    // The input split can contain single HDFS block or multiple blocks, so firstly get all the
    // blocks and then set them in the query model.
    List<CarbonHiveInputSplit> splitList;
    if (inputSplit instanceof CarbonHiveInputSplit) {
      splitList = new ArrayList<>(1);
      splitList.add((CarbonHiveInputSplit) inputSplit);
    } else {
      throw new RuntimeException("unsupported input split type: " + inputSplit);
    }
    List<TableBlockInfo> tableBlockInfoList = CarbonHiveInputSplit.createBlocks(splitList);
    queryModel.setTableBlockInfos(tableBlockInfoList);
    readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
    carbonIterator = new ChunkRowIterator(queryExecutor.execute(queryModel));
    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String colIds = conf.get("hive.io.file.readcolumn.ids");
    final String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (valueObj == null) {
      valueObj = new ArrayWritable(Writable.class, new Writable[columnTypes.size()]);
    }

    if (null != colIds && !colIds.equals("")) {
      String[] arraySelectedColId = colIds.split(",");
      columnIds = new int[arraySelectedColId.length];
      int columnId = 0;
      for (int j = 0; j < arraySelectedColId.length; j++) {
        columnId = Integer.parseInt(arraySelectedColId[j]);
        columnIds[j] = columnId;
      }
    }
  }

  @Override
  public boolean next(Void aVoid, ArrayWritable value) {
    if (carbonIterator.hasNext()) {
      Object obj = readSupport.readRow(carbonIterator.next());
      recordReaderCounter++;
      Writable[] objArray = (Writable[]) obj;
      Writable[] sysArray = new Writable[value.get().length];
      if (columnIds != null && columnIds.length > 0 && objArray.length == columnIds.length) {
        for (int i = 0; i < columnIds.length; i++) {
          sysArray[columnIds[i]] = objArray[i];
        }
        value.set(sysArray);
      } else {
        value.set(objArray);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Void createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return valueObj;
  }

  @Override
  public long getPos() {
    return recordReaderCounter;
  }

  @Override
  public float getProgress() {
    return 0;
  }

}