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
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progressable;

public class MapredCarbonOutputFormat<T> extends CarbonTableOutputFormat
    implements HiveOutputFormat<Void, T>, OutputFormat<Void, T> {

  @Override
  public RecordWriter<Void, T> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s,
      Progressable progressable) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {
    CarbonLoadModel carbonLoadModel = null;
    String encodedString = jc.get(LOAD_MODEL);
    if (encodedString != null) {
      carbonLoadModel =
          (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
    }
    if (carbonLoadModel == null) {
      carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(tableProperties, jc);
    } else {
      for (Map.Entry<Object, Object> entry : tableProperties.entrySet()) {
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getTableInfo().getFactTable()
            .getTableProperties().put(entry.getKey().toString().toLowerCase(),
            entry.getValue().toString().toLowerCase());
      }
    }
    String tablePath = FileFactory.getCarbonFile(carbonLoadModel.getTablePath()).getAbsolutePath();
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jc.get("mapred.task.id"));
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(jc, taskAttemptID);
    final boolean isHivePartitionedTable =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().isHivePartitionTable();
    PartitionInfo partitionInfo =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getPartitionInfo();
    final int partitionColumn =
        partitionInfo != null ? partitionInfo.getColumnSchemaList().size() : 0;
    String finalOutputPath = FileFactory.getCarbonFile(finalOutPath.toString()).getAbsolutePath();
    if (carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().isHivePartitionTable()) {
      carbonLoadModel.getOutputFilesInfoHolder().addToPartitionPath(finalOutputPath);
      context.getConfiguration().set("carbon.outputformat.writepath", finalOutputPath);
    }
    CarbonTableOutputFormat.setLoadModel(context.getConfiguration(), carbonLoadModel);
    org.apache.hadoop.mapreduce.RecordWriter<NullWritable, ObjectArrayWritable> re =
        super.getRecordWriter(context);
    return new FileSinkOperator.RecordWriter() {
      @Override
      public void write(Writable writable) throws IOException {
        try {
          ObjectArrayWritable objectArrayWritable = new ObjectArrayWritable();
          if (isHivePartitionedTable) {
            Object[] actualRow = ((CarbonHiveRow) writable).getData();
            Object[] newData = Arrays.copyOf(actualRow, actualRow.length + partitionColumn);
            String[] partitionValues = finalOutputPath.substring(tablePath.length()).split("/");
            for (int j = 0, i = actualRow.length; j < partitionValues.length; j++) {
              if (partitionValues[j].contains("=")) {
                newData[i++] = partitionValues[j].split("=")[1];
              }
            }
            objectArrayWritable.set(newData);
          } else {
            objectArrayWritable.set(((CarbonHiveRow) writable).getData());
          }
          re.write(NullWritable.get(), objectArrayWritable);
        } catch (InterruptedException e) {
          throw new IOException(e.getCause());
        }
      }

      @Override
      public void close(boolean b) throws IOException {
        try {
          re.close(context);
          ThreadLocalSessionInfo.setConfigurationToCurrentThread(context.getConfiguration());
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
  }

}