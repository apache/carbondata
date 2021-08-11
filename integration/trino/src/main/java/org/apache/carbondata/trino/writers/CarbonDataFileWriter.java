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

package org.apache.carbondata.trino.writers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.CarbonHiveSerDe;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;
import org.apache.carbondata.trino.impl.CarbonTableConfig;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.util.FieldSetterFactory;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;

/**
 * This class implements HiveFileWriter and it creates the carbonFileWriter
 * to write the page data
 * sent from presto.
 */
public class CarbonDataFileWriter implements FileWriter {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonDataFileWriter.class.getName());

  private final JobConf configuration;
  private final Path outPutPath;
  private final FileSinkOperator.RecordWriter recordWriter;
  private final CarbonHiveSerDe serDe;
  private final int fieldCount;
  private final Object row;
  private final SettableStructObjectInspector tableInspector;
  private final List<StructField> structFields;
  private final FieldSetterFactory.FieldSetter[] setters;

  private boolean isCommitDone;

  public CarbonDataFileWriter(Path outPutPath, List<String> inputColumnNames, Properties properties,
      JobConf configuration, TypeManager typeManager) throws SerDeException {
    requireNonNull(outPutPath, "path is null");
    // take the outputPath same as location in compliance with the carbon
    // store folder structure.
    this.outPutPath = new Path(properties.getProperty("location"));
    this.configuration = requireNonNull(configuration, "conf is null");
    List<String> columnNames = Arrays.asList(
        properties.getProperty(IOConstants.COLUMNS, "").split(CarbonCommonConstants.COMMA));
    List<Type> fileColumnTypes =
        HiveType.toHiveTypes(properties.getProperty(IOConstants.COLUMNS_TYPES, "")).stream()
            .map(hiveType -> hiveType.getType(typeManager)).collect(toList());
    this.fieldCount = columnNames.size();
    this.serDe = new CarbonHiveSerDe();
    serDe.initialize(configuration, properties);
    List<ObjectInspector> objectInspectors =
        fileColumnTypes.stream().map(HiveWriteUtils::getRowColumnInspector).collect(toList());
    this.tableInspector = getStandardStructObjectInspector(columnNames, objectInspectors);

    this.structFields = ImmutableList.copyOf(
        inputColumnNames.stream().map(tableInspector::getStructFieldRef)
            .collect(toImmutableList()));

    this.row = tableInspector.create();

    this.setters = new FieldSetterFactory.FieldSetter[structFields.size()];

    //todo get timezone
    DateTimeZone timeZone = DateTimeZone.UTC;
    FieldSetterFactory fieldSetterFactory = new FieldSetterFactory(timeZone);

    for (int i = 0; i < setters.length; i++) {
      setters[i] = fieldSetterFactory.create(tableInspector, row, structFields.get(i),
          fileColumnTypes.get(structFields.get(i).getFieldID()));
    }
    String encodedLoadModel = this.configuration.get(CarbonTableConfig.CARBON_TRINO_LOAD_MODEL);
    if (StringUtils.isNotEmpty(encodedLoadModel)) {
      this.configuration.set(CarbonTableOutputFormat.LOAD_MODEL, encodedLoadModel);
    }
    try {
      boolean compress = HiveConf.getBoolVar(this.configuration, COMPRESSRESULT);
      Object writer =
          Class.forName(MapredCarbonOutputFormat.class.getName()).getConstructor().newInstance();
      this.recordWriter =
          ((MapredCarbonOutputFormat<?>) writer).getHiveRecordWriter(this.configuration,
              this.outPutPath, Text.class, compress, properties, Reporter.NULL);
    } catch (Exception e) {
      LOG.error("error while initializing writer", e);
      throw new RuntimeException("writer class not found");
    }
  }

  @Override
  public long getWrittenBytes() {
    if (isCommitDone) {
      try {
        return outPutPath.getFileSystem(configuration).getFileStatus(outPutPath).getLen();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return 0;
  }

  @Override
  public long getSystemMemoryUsage() {
    // TODO: need to support this, Jira CARBONDATA-4038 is created
    return 0;
  }

  @Override
  public void appendRows(Page dataPage) {
    for (int position = 0; position < dataPage.getPositionCount(); position++) {
      appendRow(dataPage, position);
    }
  }

  private void appendRow(Page dataPage, int position) {
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
      throw new TrinoException(HIVE_WRITER_DATA_ERROR, e);
    }
  }

  @Override
  public void commit() {
    try {
      recordWriter.close(false);
    } catch (Exception ex) {
      LOG.error("Error while closing the record writer", ex);
      throw new RuntimeException(ex);
    }
    isCommitDone = true;
  }

  @Override
  public void rollback() {
    try {
      recordWriter.close(true);
    } catch (Exception e) {
      LOG.error("Error while closing the record writer during rollback", e);
      throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
    }
  }

  @Override
  public long getValidationCpuNanos() {
    // TODO: need to support this, Jira CARBONDATA-4038 is created
    return 0;
  }

  @Override
  public Optional<Runnable> getVerificationTask() {
    return FileWriter.super.getVerificationTask();
  }
}
