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
import java.util.Properties;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
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
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf)
      throws IOException {
  }

  private CarbonLoadModel createCarbonLoadModel(Properties tableProperties) throws IOException {
    String[] tableUniqueName = tableProperties.get("name").toString().split("\\.");
    String databaseName = tableUniqueName[0];
    String tableName = tableUniqueName[1];
    String tablePath = tableProperties.get("location").toString();
    CarbonTable carbonTable =
        CarbonTable.buildFromTablePath(tableName, databaseName, tablePath, "");
    CarbonLoadModelBuilder carbonLoadModelBuilder = new CarbonLoadModelBuilder(carbonTable);
    try {
      return carbonLoadModelBuilder
          .build(carbonTable.getTableInfo().getFactTable().getTableProperties(),
              System.currentTimeMillis(), "1");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {
    CarbonLoadModel carbonLoadModel = createCarbonLoadModel(tableProperties);
    CarbonTableOutputFormat.setLoadModel(jc, carbonLoadModel);
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jc.get("mapred.task.id"));
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(jc, taskAttemptID);
    OutputCommitter carbonOutputCommitter = super.getOutputCommitter(context);
    JobContextImpl jobContext = new JobContextImpl(jc, new JobID());
    carbonOutputCommitter.setupJob(jobContext);
    CarbonLoadModel updatedCarbonLoadModel = CarbonTableOutputFormat.getLoadModel(jc);
    org.apache.hadoop.mapreduce.RecordWriter<NullWritable, ObjectArrayWritable> re =
        super.getRecordWriter(context);
    return new FileSinkOperator.RecordWriter() {
      @Override
      public void write(Writable writable) throws IOException {
        try {
          ObjectArrayWritable objectArrayWritable = new ObjectArrayWritable();
          objectArrayWritable.set(((ArrayWritable) writable).toStrings());
          re.write(NullWritable.get(), objectArrayWritable);
          FileFactory.mkdirs(finalOutPath.getParent().toString());
          FileFactory.createNewFile(finalOutPath.toString());
        } catch (InterruptedException e) {
          throw new IOException(e.getCause());
        }
      }

      @Override
      public void close(boolean b) throws IOException {
        try {
          re.close(context);
          if (b) {
            carbonOutputCommitter.abortJob(jobContext, JobStatus.State.FAILED);
          } else {
            SegmentFileStore
                .writeSegmentFile(updatedCarbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
                    updatedCarbonLoadModel.getSegmentId(),
                    String.valueOf(updatedCarbonLoadModel.getFactTimeStamp()));
            carbonOutputCommitter.commitJob(jobContext);
          }
        } catch (InterruptedException e) {
          throw new IOException(e.getCause());
        }
      }
    };
  }

}
