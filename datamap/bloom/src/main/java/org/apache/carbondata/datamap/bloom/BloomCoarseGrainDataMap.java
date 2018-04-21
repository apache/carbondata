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

package org.apache.carbondata.datamap.bloom;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class BloomCoarseGrainDataMap extends CoarseGrainDataMap {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BloomCoarseGrainDataMap.class.getName());
  private String[] indexFilePath;
  private Set<String> indexedColumn;
  private List<BloomDMModel> bloomIndexList;
  private Multimap<String, List<BloomDMModel>> indexCol2BloomDMList;

  @Override
  public void init(DataMapModel dataMapModel) throws MemoryException, IOException {
    Path indexPath = FileFactory.getPath(dataMapModel.getFilePath());
    FileSystem fs = FileFactory.getFileSystem(indexPath);
    if (!fs.exists(indexPath)) {
      throw new IOException(
          String.format("Path %s for Bloom index dataMap does not exist", indexPath));
    }
    if (!fs.isDirectory(indexPath)) {
      throw new IOException(
          String.format("Path %s for Bloom index dataMap must be a directory", indexPath));
    }

    FileStatus[] indexFileStatus = fs.listStatus(indexPath, new PathFilter() {
      @Override public boolean accept(Path path) {
        return path.getName().endsWith(".bloomindex");
      }
    });
    indexFilePath = new String[indexFileStatus.length];
    indexedColumn = new HashSet<String>();
    bloomIndexList = new ArrayList<BloomDMModel>();
    indexCol2BloomDMList = ArrayListMultimap.create();
    for (int i = 0; i < indexFileStatus.length; i++) {
      indexFilePath[i] = indexFileStatus[i].getPath().toString();
      String indexCol = StringUtils.substringBetween(indexFilePath[i], ".carbondata.",
          ".bloomindex");
      indexedColumn.add(indexCol);
      bloomIndexList.addAll(readBloomIndex(indexFilePath[i]));
      indexCol2BloomDMList.put(indexCol, readBloomIndex(indexFilePath[i]));
    }
    LOGGER.info("find bloom index datamap for column: "
        + StringUtils.join(indexedColumn, ", "));
  }

  private List<BloomDMModel> readBloomIndex(String indexFile) throws IOException {
    LOGGER.info("read bloom index from file: " + indexFile);
    List<BloomDMModel> bloomDMModelList = new ArrayList<BloomDMModel>();
    DataInputStream dataInStream = null;
    ObjectInputStream objectInStream = null;
    try {
      dataInStream = FileFactory.getDataInputStream(indexFile, FileFactory.getFileType(indexFile));
      objectInStream = new ObjectInputStream(dataInStream);
      try {
        BloomDMModel model = null;
        while ((model = (BloomDMModel) objectInStream.readObject()) != null) {
          LOGGER.info("read bloom index: " + model);
          bloomDMModelList.add(model);
        }
      } catch (EOFException e) {
        LOGGER.info("read " + bloomDMModelList.size() + " bloom indices from " + indexFile);
      }
      return bloomDMModelList;
    } catch (ClassNotFoundException e) {
      LOGGER.error("Error occrus while reading bloom index");
      throw new RuntimeException("Error occrus while reading bloom index", e);
    } finally {
      CarbonUtil.closeStreams(objectInStream, dataInStream);
    }
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions) throws IOException {
    List<Blocklet> hitBlocklets = new ArrayList<Blocklet>();
    if (filterExp == null) {
      return null;
    }

    List<BloomQueryModel> bloomQueryModels = getQueryValue(filterExp.getFilterExpression());

    for (BloomQueryModel bloomQueryModel : bloomQueryModels) {
      LOGGER.info("prune blocklet for query: " + bloomQueryModel);
      for (List<BloomDMModel> bloomDMModels : indexCol2BloomDMList.get(
          bloomQueryModel.columnName)) {
        for (BloomDMModel bloomDMModel : bloomDMModels) {
          boolean scanRequired = bloomDMModel.getBloomFilter().mightContain(
              convertValueToBytes(bloomQueryModel.dataType, bloomQueryModel.filterValue));
          if (scanRequired) {
            LOGGER.info(String.format(
                "BloomCoarseGrainDataMap: Need to scan block#%s -> blocklet#%s",
                bloomDMModel.getBlockId(), String.valueOf(bloomDMModel.getBlockletNo())));
            Blocklet blocklet = new Blocklet(bloomDMModel.getBlockId(),
                String.valueOf(bloomDMModel.getBlockletNo()));
            hitBlocklets.add(blocklet);
          } else {
            LOGGER.info(String.format(
                "BloomCoarseGrainDataMap: Skip scan block#%s -> blocklet#%s",
                bloomDMModel.getBlockId(), String.valueOf(bloomDMModel.getBlockletNo())));
          }
        }
      }
    }

    return hitBlocklets;
  }

  private byte[] convertValueToBytes(DataType dataType, Object value) {
    try {
      if (dataType == DataTypes.STRING) {
        if (value instanceof byte[]) {
          return (byte[]) value;
        } else {
          return String.valueOf(value).getBytes("utf-8");
        }
      } else {
        return CarbonUtil.getValueAsBytes(dataType, value);
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Error occurs while converting " + value + " to " + dataType, e);
    }
  }

  private List<BloomQueryModel> getQueryValue(Expression expression) {
    List<BloomQueryModel> queryModels = new ArrayList<BloomQueryModel>();
    if (expression instanceof EqualToExpression) {
      Expression left = ((EqualToExpression) expression).getLeft();
      Expression right = ((EqualToExpression) expression).getRight();
      String column;
      DataType dataType;
      Object value;
      if (left instanceof ColumnExpression && right instanceof LiteralExpression) {
        column = ((ColumnExpression) left).getColumnName();
        if (indexedColumn.contains(column)) {
          dataType = ((ColumnExpression) left).getDataType();
          value = ((LiteralExpression) right).getLiteralExpValue();
          BloomQueryModel bloomQueryModel = new BloomQueryModel(column, dataType, value);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      } else if (left instanceof LiteralExpression && right instanceof ColumnExpression) {
        column = ((ColumnExpression) right).getColumnName();
        if (indexedColumn.contains(column)) {
          dataType = ((ColumnExpression) right).getDataType();
          value = ((LiteralExpression) left).getLiteralExpValue();
          BloomQueryModel bloomQueryModel = new BloomQueryModel(column, dataType, value);
          queryModels.add(bloomQueryModel);
        }
        return queryModels;
      }
    }

    for (Expression child : expression.getChildren()) {
      queryModels.addAll(getQueryValue(child));
    }
    return queryModels;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  @Override
  public void clear() {
    bloomIndexList.clear();
    bloomIndexList = null;
  }

  static class BloomQueryModel {
    private String columnName;
    private DataType dataType;
    private Object filterValue;

    public BloomQueryModel(String columnName, DataType dataType, Object filterValue) {
      this.columnName = columnName;
      this.dataType = dataType;
      this.filterValue = filterValue;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BloomQueryModel{");
      sb.append("columnName='").append(columnName).append('\'');
      sb.append(", dataType=").append(dataType);
      sb.append(", filterValue=").append(filterValue);
      sb.append('}');
      return sb.toString();
    }
  }
}
