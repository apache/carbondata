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
package org.apache.carbondata.spark.testsuite.dataload

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.rdd.{DataLoadPartitionCoalescer, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.scalatest.BeforeAndAfterAll

class TestDataLoadPartitionCoalescer extends QueryTest with BeforeAndAfterAll {
  var nodeList: Array[String] = _

  class DummyPartition(val index: Int,
                       rawSplit: FileSplit) extends Partition {
    val serializableHadoopSplit = new SerializableWritable(rawSplit)
  }

  class Dummy(sc: SparkContext, partitions: Array[Partition]) extends RDD[Row](sc, Nil) {
    override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
      new Iterator[Row] {
        var isFirst = true;
        override def hasNext: Boolean = isFirst;

        override def next(): Row = {
          isFirst = false
          new GenericRow(Array[Any]())
        }
      }
    }

    override protected def getPartitions: Array[Partition] = partitions

    override protected def getPreferredLocations(split: Partition): Seq[String] = {
      split.asInstanceOf[DummyPartition].serializableHadoopSplit.value.getLocations.toSeq
    }

  }

  override def beforeAll: Unit = {
    nodeList = Array("host1", "host2", "host3")

  }

  def createPartition(index: Int, file: String, hosts: Array[String]) : Partition = {
    new DummyPartition(index, new FileSplit(new Path(file), 0, 1, hosts))
  }

  def repartition(parts: Array[Partition]): Array[Partition] = {
    new DataLoadPartitionCoalescer(new Dummy(sqlContext.sparkContext, parts), nodeList).run
  }

  def checkPartition(prevParts: Array[Partition], parts: Array[Partition]): Unit = {
    DataLoadPartitionCoalescer.checkPartition(prevParts, parts)
  }

  test("test number of partitions is more than nodes's") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array("host1", "host2", "host3")),
      createPartition(1, "2.csv", Array("host1", "host2", "host3")),
      createPartition(2, "3.csv", Array("host1", "host2", "host3")),
      createPartition(3, "4.csv", Array("host1", "host2", "host3")),
      createPartition(4, "5.csv", Array("host1", "host2", "host3"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 3)
    checkPartition(prevParts, parts)
  }

  test("test number of partitions equals nodes's") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array("host1", "host2", "host3")),
      createPartition(1, "2.csv", Array("host1", "host2", "host3")),
      createPartition(2, "3.csv", Array("host1", "host2", "host3"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 3)
    checkPartition(prevParts, parts)
  }

  test("test number of partitions is less than nodes's") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array("host1", "host2", "host3")),
      createPartition(1, "2.csv", Array("host1", "host2", "host3"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 2)
    checkPartition(prevParts, parts)
  }

  test("all partitions are locality") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array("host1", "host2", "host3")),
      createPartition(1, "2.csv", Array("host1", "host2", "host3"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 2)
    checkPartition(prevParts, parts)
  }

  test("part of partitions are locality1") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array("host1", "host2", "host3")),
      createPartition(1, "2.csv", Array("host1", "host2", "host4")),
      createPartition(2, "3.csv", Array("host4", "host5", "host6"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 3)
    checkPartition(prevParts, parts)
  }

  test("part of partitions are locality2") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array("host1", "host2", "host3")),
      createPartition(1, "2.csv", Array("host1", "host2", "host4")),
      createPartition(2, "3.csv", Array("host3", "host5", "host6"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 3)
    checkPartition(prevParts, parts)
  }

  test("part of partitions are locality3") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array("host1", "host2", "host7")),
      createPartition(1, "2.csv", Array("host1", "host2", "host4")),
      createPartition(2, "3.csv", Array("host4", "host5", "host6"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 3)
    checkPartition(prevParts, parts)
  }

  test("all partition are not locality") {
    val prevParts = Array[Partition](
      createPartition(0, "1.csv", Array()),
      createPartition(1, "2.csv", Array()),
      createPartition(2, "3.csv", Array("host4", "host5", "host6"))
    )
    val parts = repartition(prevParts)
    assert(parts.size == 3)
    checkPartition(prevParts, parts)
  }

  override def afterAll {
  }
}

