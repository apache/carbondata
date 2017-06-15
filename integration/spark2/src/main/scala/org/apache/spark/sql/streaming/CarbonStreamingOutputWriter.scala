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

package org.apache.spark.sql.streaming

import org.apache.carbondata.hadoop.streaming.{CarbonStreamingOutputFormat, CarbonStreamingRecordWriter}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.Row


class CarbonStreamingOutputWriter (
    path: String,
    context: TaskAttemptContext)
    extends OutputWriter {

  private[this] val buffer = new Text()

  private val recordWriter: CarbonStreamingRecordWriter[NullWritable, Text] = {

    val outputFormat = new CarbonStreamingOutputFormat[NullWritable, Text] () {

      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String) : Path = {
        new Path(path)
      }

    /*
     May need to override
     def getOutputCommiter(c: TaskAttemptContext): OutputCommitter = {
      null
    }
    */

    }

    outputFormat.getRecordWriter(context).asInstanceOf[CarbonStreamingRecordWriter[NullWritable, Text]]
  }

  override def write(row: Row): Unit = {

    throw new UnsupportedOperationException("call writeInternal")

  }

  override protected [sql] def writeInternal(row: InternalRow): Unit = {

    val utf8string = row.getUTF8String(0)

    buffer.set(utf8string.getBytes)

    recordWriter.write(NullWritable.get(), buffer)

  }

  def getpath: String = path

  override def close(): Unit = {

    recordWriter.close(context)

  }

  def flush(): Unit = {

    recordWriter.flush()

  }

  def getPos(): Long = {

    recordWriter.getOffset()

  }

  def commit(finalCommit: Boolean): Unit = {

    recordWriter.commit(finalCommit)

  }
}
