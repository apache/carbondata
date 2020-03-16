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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.hive.CarbonHiveSerDe;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveFileWriter;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveWriteUtils;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;

/**
 * This class implements HiveFileWriter and it creates the carbonFileWriter to write the age data
 * sent from presto.
 */
public class CarbonDataFileWriter implements HiveFileWriter {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonDataFileWriter.class.getName());

  private final JobConf configuration;
  private Path outPutPath;
  private final FileSinkOperator.RecordWriter recordWriter;
  private final CarbonHiveSerDe serDe;
  private final int fieldCount;
  private final Object row;
  private final SettableStructObjectInspector tableInspector;
  private final List<StructField> structFields;
  private final HiveWriteUtils.FieldSetter[] setters;

  private boolean isCommitDone;

  public CarbonDataFileWriter(Path outPutPath, List<String> inputColumnNames, Properties properties,
      JobConf configuration, TypeManager typeManager) throws SerDeException {
    this.outPutPath = requireNonNull(outPutPath, "path is null");
    this.outPutPath = new Path(properties.getProperty("location"));
    outPutPath = new Path(properties.getProperty("location"));
    this.configuration = requireNonNull(configuration, "conf is null");
    List<String> columnNames = Arrays
        .asList(properties.getProperty(IOConstants.COLUMNS, "").split(CarbonCommonConstants.COMMA));
    List<Type> fileColumnTypes =
        HiveType.toHiveTypes(properties.getProperty(IOConstants.COLUMNS_TYPES, "")).stream()
            .map(hiveType -> hiveType.getType(typeManager)).collect(toList());
    fieldCount = columnNames.size();
    serDe = new CarbonHiveSerDe();
    serDe.initialize(configuration, properties);
    tableInspector = (ArrayWritableObjectInspector) serDe.getObjectInspector();

    structFields = ImmutableList.copyOf(
        inputColumnNames.stream().map(tableInspector::getStructFieldRef)
            .collect(toImmutableList()));

    row = tableInspector.create();

    setters = new HiveWriteUtils.FieldSetter[structFields.size()];
    for (int i = 0; i < setters.length; i++) {
      setters[i] = HiveWriteUtils.createFieldSetter(tableInspector, row, structFields.get(i),
          fileColumnTypes.get(structFields.get(i).getFieldID()));
    }
    try {
      boolean compress = HiveConf.getBoolVar(configuration, COMPRESSRESULT);
      Object writer =
          Class.forName(MapredCarbonOutputFormat.class.getName()).getConstructor().newInstance();
      recordWriter = ((MapredCarbonOutputFormat<?>) writer)
          .getHiveRecordWriter(this.configuration, outPutPath, Text.class, compress,
              properties, Reporter.NULL);
    } catch (Exception e) {
      LOG.error("error while initializing writer", e);
      throw new RuntimeException("writer class not found");
    }
  }

  @Override public long getWrittenBytes() {
    if (isCommitDone) {
      try {
        return outPutPath.getFileSystem(configuration).getFileStatus(outPutPath).getLen();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return 0;
  }

  @Override public long getSystemMemoryUsage() {
    return 0;
  }

  @Override public void appendRows(Page dataPage) {
    for (int position = 0; position < dataPage.getPositionCount(); position++) {
      appendRow(dataPage, position);
    }
  }

  public void appendRow(Page dataPage, int position) {
    for (int field = 0; field < fieldCount; field++) {
      Block block = dataPage.getBlock(field);
      if (block.isNull(position)) {
        tableInspector.setStructFieldData(row, structFields.get(field), null);
      } else {
        setters[field].setField(block, position);
      }
    }

    try {
      recordWriter.write(serDe.serialize(row, tableInspector));
    } catch (SerDeException | IOException e) {
      throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
    }
  }

  @Override public void commit() {
    try {
      recordWriter.close(false);
    } catch (Exception ex) {
      LOG.error("Error while closing the record writer", ex);
      throw new RuntimeException(ex);
    }
    isCommitDone = true;
  }

  @Override public void rollback() {

  }

  @Override public long getValidationCpuNanos() {
    return 0;
  }
}
