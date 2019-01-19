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

package org.apache.carbondata.presto;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.stream.StreamRecordReader;
import org.apache.carbondata.presto.impl.CarbonLocalMultiBlockSplit;
import org.apache.carbondata.presto.readers.PrestoVectorBlockBuilder;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import com.facebook.presto.hadoop.$internal.com.google.common.base.Throwables;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

import static com.google.common.base.Preconditions.checkState;

/**
 * Carbondata Page Source class for custom Carbondata RecordSet Iteration.
 */
class CarbondataPageSource implements ConnectorPageSource {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbondataPageSource.class.getName());

  private HiveSplit split;
  private CarbonTable carbonTable;
  private String queryId;
  private Configuration hadoopConf;
  private FileFormat fileFormat;
  private List<ColumnHandle> columnHandles;
  private int columnCount = 0;
  private boolean closed;
  private long sizeOfData = 0;
  private int batchId;
  private long nanoStart;
  private long nanoEnd;
  private CarbonDictionaryDecodeReadSupport readSupport;

  // columnar format split
  private PrestoCarbonVectorizedRecordReader vectorReader;
  private boolean isDirectVectorFill;

  // row format split
  private StreamRecordReader rowReader;
  private StructField[] fields;
  private int batchSize = 100;
  private Dictionary[] dictionaries;
  private DataType[] dataTypes;
  private boolean isFrstPage = true;

  CarbondataPageSource(CarbonTable carbonTable, String queryId, HiveSplit split,
      List<ColumnHandle> columnHandles, Configuration hadoopConf, boolean isDirectVectorFill) {
    this.carbonTable = carbonTable;
    this.queryId = queryId;
    this.split = split;
    this.columnHandles = columnHandles;
    this.hadoopConf = hadoopConf;
    this.isDirectVectorFill = isDirectVectorFill;
    initialize();
  }

  private void initialize() {
    CarbonMultiBlockSplit carbonInputSplit = CarbonLocalMultiBlockSplit
        .convertSplit(split.getSchema().getProperty("carbonSplit"));
    fileFormat = carbonInputSplit.getFileFormat();
    if (fileFormat.ordinal() == FileFormat.ROW_V1.ordinal()) {
      initializeForRow();
    } else {
      initializeForColumnar();
    }
  }

  private void initializeForColumnar() {
    readSupport = new CarbonDictionaryDecodeReadSupport();
    vectorReader = createReaderForColumnar(split, columnHandles, readSupport, hadoopConf);
  }

  private void initializeForRow() {
    QueryModel queryModel = createQueryModel(split, columnHandles, hadoopConf);
    rowReader = new StreamRecordReader(queryModel, false);
    List<ProjectionDimension> queryDimension = queryModel.getProjectionDimensions();
    List<ProjectionMeasure> queryMeasures = queryModel.getProjectionMeasures();
    fields = new StructField[queryDimension.size() + queryMeasures.size()];
    for (int i = 0; i < queryDimension.size(); i++) {
      ProjectionDimension dim = queryDimension.get(i);
      if (dim.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dim.getDimension().getDataType());
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(), generator.getReturnType());
      } else if (!dim.getDimension().hasEncoding(Encoding.DICTIONARY)) {
        fields[dim.getOrdinal()] =
            new StructField(dim.getColumnName(), dim.getDimension().getDataType());
      } else if (dim.getDimension().isComplex()) {
        fields[dim.getOrdinal()] =
            new StructField(dim.getColumnName(), dim.getDimension().getDataType());
      } else {
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(), DataTypes.INT);
      }
    }

    for (ProjectionMeasure msr : queryMeasures) {
      DataType dataType = msr.getMeasure().getDataType();
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.SHORT || dataType == DataTypes.INT
          || dataType == DataTypes.LONG) {
        fields[msr.getOrdinal()] =
            new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
      } else if (DataTypes.isDecimal(dataType)) {
        fields[msr.getOrdinal()] =
            new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
      } else {
        fields[msr.getOrdinal()] = new StructField(msr.getColumnName(), DataTypes.DOUBLE);
      }
    }

    this.columnCount = columnHandles.size();
    readSupport = new CarbonDictionaryDecodeReadSupport();
    readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
    this.dictionaries = readSupport.getDictionaries();
    this.dataTypes = readSupport.getDataTypes();

  }

  @Override public long getCompletedBytes() {
    return sizeOfData;
  }

  @Override public long getReadTimeNanos() {
    return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
  }

  @Override public boolean isFinished() {
    return closed;
  }

  @Override public Page getNextPage() {
    if (fileFormat.ordinal() == FileFormat.ROW_V1.ordinal()) {
      return getNextPageForRow();
    } else {
      return getNextPageForColumnar();
    }
  }

  private Page getNextPageForColumnar() {
    if (nanoStart == 0) {
      nanoStart = System.nanoTime();
    }
    CarbonVectorBatch columnarBatch = null;
    int batchSize = 0;
    try {
      batchId++;
      if (vectorReader.nextKeyValue()) {
        Object vectorBatch = vectorReader.getCurrentValue();
        if (vectorBatch instanceof CarbonVectorBatch) {
          columnarBatch = (CarbonVectorBatch) vectorBatch;
          batchSize = columnarBatch.numRows();
          if (batchSize == 0) {
            close();
            return null;
          }
        }
      } else {
        close();
        return null;
      }
      if (columnarBatch == null) {
        return null;
      }

      Block[] blocks = new Block[columnHandles.size()];
      for (int column = 0; column < blocks.length; column++) {
        blocks[column] = new LazyBlock(batchSize, new CarbondataBlockLoader(column));
      }
      Page page = new Page(batchSize, blocks);
      return page;
    } catch (PrestoException e) {
      closeWithSuppression(e);
      throw e;
    } catch (RuntimeException | InterruptedException | IOException e) {
      closeWithSuppression(e);
      throw new CarbonDataLoadingException("Exception when creating the Carbon data Block", e);
    }
  }

  private Page getNextPageForRow() {
    if (isFrstPage) {
      isFrstPage = false;
      initialReaderForRow();
    }

    if (nanoStart == 0) {
      nanoStart = System.nanoTime();
    }
    int count = 0;
    try {
      Block[] blocks = new Block[columnCount];
      CarbonColumnVectorImpl[] columns = new CarbonColumnVectorImpl[columnCount];
      for (int i = 0; i < columnCount; ++i) {
        columns[i] = CarbonVectorBatch
            .createDirectStreamReader(batchSize, dataTypes[i], fields[i], dictionaries[i]);
      }

      while (rowReader.nextKeyValue()) {
        Object[] values = (Object[]) rowReader.getCurrentValue();
        for (int index = 0; index < columnCount; index++) {
          columns[index].putObject(count, values[index]);
        }
        count++;
        if (count == batchSize) {
          break;
        }
      }
      if (count == 0) {
        close();
        return null;
      } else {
        for (int index = 0; index < columnCount; index++) {
          blocks[index] = ((PrestoVectorBlockBuilder) columns[index]).buildBlock();
          sizeOfData += blocks[index].getSizeInBytes();
        }
      }
      return new Page(count, blocks);
    } catch (PrestoException e) {
      closeWithSuppression(e);
      throw e;
    } catch (RuntimeException | InterruptedException | IOException e) {
      closeWithSuppression(e);
      throw new CarbonDataLoadingException("Exception when creating the Carbon data Block", e);
    }
  }

  private void initialReaderForRow() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
    String jobTrackerId = formatter.format(new Date());
    TaskAttemptID attemptId = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, 0, 0);
    TaskAttemptContextImpl attemptContext =
        new TaskAttemptContextImpl(FileFactory.getConfiguration(), attemptId);
    CarbonMultiBlockSplit carbonInputSplit = CarbonLocalMultiBlockSplit
        .convertSplit(split.getSchema().getProperty("carbonSplit"));
    try {
      rowReader.initialize(carbonInputSplit, attemptContext);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public long getSystemMemoryUsage() {
    return sizeOfData;
  }

  @Override public void close() {
    // some hive input formats are broken and bad things can happen if you close them multiple times
    if (closed) {
      return;
    }
    closed = true;
    try {
      if (vectorReader != null) {
        vectorReader.close();
      }
      if (rowReader != null) {
        rowReader.close();
      }
      nanoEnd = System.nanoTime();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  private void closeWithSuppression(Throwable throwable) {
    Objects.requireNonNull(throwable, "throwable is null");
    try {
      close();
    } catch (RuntimeException e) {
      // Self-suppression not permitted
      LOGGER.error(e.getMessage(), e);
      if (throwable != e) {
        throwable.addSuppressed(e);
      }
    }
  }

  /**
   * Create vector reader using the split.
   */
  private PrestoCarbonVectorizedRecordReader createReaderForColumnar(HiveSplit carbonSplit,
      List<? extends ColumnHandle> columns, CarbonDictionaryDecodeReadSupport readSupport,
      Configuration conf) {
    QueryModel queryModel = createQueryModel(carbonSplit, columns, conf);
    if (isDirectVectorFill) {
      queryModel.setDirectVectorFill(true);
      queryModel.setPreFetchData(false);
    }
    QueryExecutor queryExecutor =
        QueryExecutorFactory.getQueryExecutor(queryModel, new Configuration());
    try {
      CarbonIterator iterator = queryExecutor.execute(queryModel);
      readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
      PrestoCarbonVectorizedRecordReader reader =
          new PrestoCarbonVectorizedRecordReader(queryExecutor, queryModel,
              (AbstractDetailQueryResultIterator) iterator, readSupport);
      reader.setTaskId(Long.parseLong(carbonSplit.getSchema().getProperty("index")));
      return reader;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create reader ", e);
    }
  }

  /**
   * @param carbondataSplit
   * @param columns
   * @return
   */
  private QueryModel createQueryModel(HiveSplit carbondataSplit,
      List<? extends ColumnHandle> columns, Configuration conf) {

    try {
      CarbonProjection carbonProjection = getCarbonProjection(columns);
      conf.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
      String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
      CarbonTableInputFormat
          .setTransactionalTable(conf, carbonTable.getTableInfo().isTransactionalTable());
      CarbonTableInputFormat.setTableInfo(conf, carbonTable.getTableInfo());
      conf.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
      conf.set("query.id", queryId);
      JobConf jobConf = new JobConf(conf);
      CarbonTableInputFormat carbonTableInputFormat = createInputFormat(jobConf, carbonTable,
          PrestoFilterUtil.parseFilterExpression(carbondataSplit.getEffectivePredicate()),
          carbonProjection);
      TaskAttemptContextImpl hadoopAttemptContext =
          new TaskAttemptContextImpl(jobConf, new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
      CarbonMultiBlockSplit carbonInputSplit = CarbonLocalMultiBlockSplit
          .convertSplit(carbondataSplit.getSchema().getProperty("carbonSplit"));
      QueryModel queryModel =
          carbonTableInputFormat.createQueryModel(carbonInputSplit, hadoopAttemptContext);
      queryModel.setQueryId(queryId);
      queryModel.setVectorReader(true);
      queryModel.setStatisticsRecorder(
          CarbonTimeStatisticsFactory.createExecutorRecorder(queryModel.getQueryId()));

      List<TableBlockInfo> tableBlockInfoList =
          CarbonInputSplit.createBlocks(carbonInputSplit.getAllSplits());
      queryModel.setTableBlockInfos(tableBlockInfoList);
      return queryModel;
    } catch (IOException e) {
      throw new RuntimeException("Unable to get the Query Model ", e);
    }
  }

  /**
   * @param conf
   * @param carbonTable
   * @param filterExpression
   * @param projection
   * @return
   */
  private CarbonTableInputFormat<Object> createInputFormat(Configuration conf,
      CarbonTable carbonTable, Expression filterExpression, CarbonProjection projection) {

    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
    try {
      CarbonTableInputFormat
          .setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath()));
      CarbonTableInputFormat
          .setDatabaseName(conf, identifier.getCarbonTableIdentifier().getDatabaseName());
      CarbonTableInputFormat
          .setTableName(conf, identifier.getCarbonTableIdentifier().getTableName());
    } catch (Exception e) {
      throw new RuntimeException("Unable to create the CarbonTableInputFormat", e);
    }
    CarbonTableInputFormat.setFilterPredicates(conf, filterExpression);
    CarbonTableInputFormat.setColumnProjection(conf, projection);

    return format;
  }

  /**
   * @param columns
   * @return
   */
  private CarbonProjection getCarbonProjection(List<? extends ColumnHandle> columns) {
    CarbonProjection carbonProjection = new CarbonProjection();
    // Convert all columns handles
    ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add(Types.checkType(handle, HiveColumnHandle.class, "handle"));
      carbonProjection.addColumn(((HiveColumnHandle) handle).getName());
    }
    return carbonProjection;
  }

  /**
   * Lazy Block Implementation for the Carbondata
   */
  private final class CarbondataBlockLoader implements LazyBlockLoader<LazyBlock> {
    private final int expectedBatchId = batchId;
    private final int columnIndex;
    private boolean loaded;

    CarbondataBlockLoader(int columnIndex) {
      this.columnIndex = columnIndex;
    }

    @Override public final void load(LazyBlock lazyBlock) {
      if (loaded) {
        return;
      }
      checkState(batchId == expectedBatchId);
      try {
        vectorReader.getColumnarBatch().column(columnIndex).loadPage();
        PrestoVectorBlockBuilder blockBuilder =
            (PrestoVectorBlockBuilder) vectorReader.getColumnarBatch().column(columnIndex);
        blockBuilder.setBatchSize(lazyBlock.getPositionCount());
        Block block = blockBuilder.buildBlock();
        sizeOfData += block.getSizeInBytes();
        lazyBlock.setBlock(block);
      } catch (Exception e) {
        throw new CarbonDataLoadingException("Error in Reading Data from Carbondata ", e);
      }
      loaded = true;
    }
  }

}