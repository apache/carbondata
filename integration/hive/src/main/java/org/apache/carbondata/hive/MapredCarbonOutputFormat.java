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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.util.CarbonProperties;
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
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progressable;

public class MapredCarbonOutputFormat<T> extends CarbonTableOutputFormat
    implements HiveOutputFormat<Void, T>, OutputFormat<Void, T> {

  static {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "hive");
  }

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
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(jc);
    CarbonLoadModel carbonLoadModel = null;
    // Try to get loadmodel from JobConf.
    String encodedString = jc.get(LOAD_MODEL);
    if (encodedString != null) {
      carbonLoadModel =
          (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
    } else {
      // Try to get loadmodel from Container environment.
      encodedString = System.getenv("carbon");
      if (encodedString != null) {
        carbonLoadModel =
            (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
      } else {
        carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(tableProperties, jc);
      }
    }
    for (Map.Entry<Object, Object> entry : tableProperties.entrySet()) {
      carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getTableInfo().getFactTable()
          .getTableProperties()
          .put(entry.getKey().toString().toLowerCase(), entry.getValue().toString().toLowerCase());
    }
    String tablePath = FileFactory.getCarbonFile(carbonLoadModel.getTablePath()).getAbsolutePath();
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jc.get("mapred.task.id"));
    // taskAttemptID will be null when the insert job is fired from presto. Presto send the JobConf
    // and since presto does not use the MR framework for execution, the mapred.task.id will be
    // null, so prepare a new ID.
    if (taskAttemptID == null) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
      String jobTrackerId = formatter.format(new Date());
      taskAttemptID = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, 0, 0);
      // update the app name here, as in this class by default it will written by Hive
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "presto");
    } else {
      carbonLoadModel.setTaskNo("" + taskAttemptID.getTaskID().getId());
    }
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(jc, taskAttemptID);
    final boolean isHivePartitionedTable =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().isHivePartitionTable();
    PartitionInfo partitionInfo =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getPartitionInfo();
    final int partitionColumn =
        partitionInfo != null ? partitionInfo.getColumnSchemaList().size() : 0;
    if (carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().isHivePartitionTable()) {
      carbonLoadModel.getMetrics().addToPartitionPath(finalOutPath.toString());
      context.getConfiguration().set("carbon.outputformat.writepath", finalOutPath.toString());
    }
    CarbonTableOutputFormat.setLoadModel(jc, carbonLoadModel);
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
            String[] partitionValues = finalOutPath.toString().substring(tablePath.length())
                .split("/");
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