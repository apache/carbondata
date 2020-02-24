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

package org.apache.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}

/**
 * support data skew scenario
 * copy from spark: RangePartitioner
 *
 * RangePartitioner:
 * the rangeBounds are the distinct values, each rangeBound has a partition.
 * so for the data skew scenario, some partitions will include more data.
 *
 * DataSkewRangePartitioner:
 * the rangeBounds are also the distinct values, but it calculates the skew weight.
 * So some rangeBounds maybe have more than one partitions.
 *
 * for example, split following CSV file to 5 partitions by col2:
 * ---------------
 * col1,col2
 * 1,
 * 2,
 * 3,
 * 4,
 * 5,e
 * 6,f
 * 7,g
 * 8,h
 * 9,i
 * 10,j
 * ---------------
 * RangePartitioner will give the following rangeBounds.
 * -------------------------------------------------
 * | range bound| partition range| number of values|
 * -------------------------------------------------
 * | null       | <= null        | 4               |
 * | e          | (null, e]      | 1               |
 * | f          | (e, f]         | 1               |
 * | h          | (f, h]         | 2               |
 * |            | > h            | 2               |
 * -------------------------------------------------
 *
 * DataSkewRangePartitioner will give the following rangeBounds.
 * --------------------------------------------------------------
 * | range bound| skew weight| partition range| number of values|
 * --------------------------------------------------------------
 * | null       | 2          | <= null        | 2(4/2)          |
 * | f          |            | (null, f]      | 2               |
 * | h          |            | (f, h]         | 2               |
 * |            |            | > h            | 2               |
 * |            |            | <= null        | 2(4/2)          |
 * --------------------------------------------------------------
 * The skew weight of range bound "null" is 2.
 * So it will start two tasks for range bound "null" to create two partitions.
 * For a range bound, the number of final partitions is the same as the skew weight.
 */
class DataSkewRangePartitioner[K: Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    withoutSkew: Boolean)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  // dataSkewCount: how many bounds happened data skew
  // dataSkewIndex: the index of data skew bounds
  // dataSkewNum: how many partition of each data skew bound
  // Min and Max values of complete range
  var (rangeBounds: Array[K], skewCount: Int, skewIndexes: Array[Int],
  skewWeights: Array[Int]) = {
    if (partitions <= 1) {
      (Array.empty[K], 0, Array.empty[Int], Array.empty[Int])
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        (Array.empty[K], 0, Array.empty[Int], Array.empty[Int], Array.empty[K])
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        // In case of compaction we do not need the skew handled ranges so we use RangePartitioner,
        // but we require the overall minmax for creating the separate ranges.
        // withoutSkew = true for Compaction only
        if (withoutSkew == false) {
          determineBounds(candidates, partitions, false)
        } else {
          var ranges = RangePartitioner.determineBounds(candidates, partitions)
          var otherRangeParams = determineBounds(candidates, partitions, true)
          (ranges, otherRangeParams._2, otherRangeParams._3,
            otherRangeParams._4)
        }
      }
    }
  }

  def determineBounds(
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int,
      withoutSkew: Boolean): (Array[K], Int, Array[Int], Array[Int]) = {
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gteq(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }

    if (bounds.size >= 2) {
      combineDataSkew(bounds)
    } else {
      (bounds.toArray, 0, Array.empty[Int], Array.empty[Int])
    }
  }

  def combineDataSkew(bounds: ArrayBuffer[K]): (Array[K], Int, Array[Int], Array[Int]) = {
    val finalBounds = ArrayBuffer.empty[K]
    var preBound = bounds(0)
    finalBounds += preBound
    val dataSkewIndexTmp = ArrayBuffer.empty[Int]
    val dataSkewNumTmp = ArrayBuffer.empty[Int]
    var dataSkewCountTmp = 1
    (1 until bounds.size).foreach { index =>
      val bound = bounds(index)
      if (ordering.equiv(bound, preBound)) {
        if (dataSkewCountTmp == 1) {
          dataSkewIndexTmp += (finalBounds.size - 1)
        }
        dataSkewCountTmp += 1
      } else {
        finalBounds += bound
        preBound = bound
        if (dataSkewCountTmp > 1) {
          dataSkewNumTmp += dataSkewCountTmp
          dataSkewCountTmp = 1
        }
      }
    }
    if (dataSkewCountTmp > 1) {
      dataSkewNumTmp += dataSkewCountTmp
    }
    if (dataSkewIndexTmp.size > 0) {
      (finalBounds.toArray, dataSkewIndexTmp.size, dataSkewIndexTmp.toArray, dataSkewNumTmp
        .toArray)
    } else {
      (finalBounds.toArray, 0, Array.empty[Int], Array.empty[Int])
    }
  }

  private var skewPartitions: Int = if (skewCount == 0) {
    0
  } else {
    skewWeights.map(_ - 1).sum
  }

  def numPartitions: Int = rangeBounds.length + 1 + skewPartitions

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]
  private var skewIndexesLen = 0
  private var dsps: Array[DataSkewPartitioner] = null

  def initialize(): Unit = {
    if (skewCount > 0) {
      skewIndexesLen = skewIndexes.length
      dsps = new Array[DataSkewPartitioner](skewIndexesLen)
      var previousPart = rangeBounds.length
      for (i <- 0 until skewIndexesLen) {
        dsps(i) = new DataSkewPartitioner(skewIndexes(i), previousPart, skewWeights(i))
        previousPart = previousPart + skewWeights(i) - 1
      }
    }
  }

  private var needInit = true

  def getPartition(key: Any): Int = {
    if (needInit) {
      needInit = false
      initialize()
    }
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      val len = rangeBounds.length
      while (partition < len && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition - 1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (skewCount == 0) {
      partition
    } else {
      getDataSkewPartition(partition)
    }
  }

  def getDataSkewPartition(partition: Int): Int = {
    var index = -1
    if (partition <= skewIndexes(skewIndexesLen - 1) && partition >= skewIndexes(0)) {
      for (i <- 0 until skewIndexesLen) {
        if (skewIndexes(i) == partition) {
          index = i
        }
      }
    }
    if (index == -1) {
      partition
    } else {
      nextPartition(index)
    }
  }

  def nextPartition(index: Int): Int = {
    dsps(index).nextPartition()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case r: DataSkewRangePartitioner[_, _] =>
        r.rangeBounds.sameElements(rangeBounds)
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    val len = rangeBounds.length
    while (i < len) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    Utils.tryOrIOException {
      val sfactory = SparkEnv.get.serializer
      sfactory match {
        case js: JavaSerializer => out.defaultWriteObject()
        case _ =>
          out.writeInt(skewCount)
          out.writeInt(skewPartitions)
          if (skewCount > 0) {
            out.writeObject(skewIndexes)
            out.writeObject(skewWeights)
          }
          out.writeObject(ordering)
          out.writeObject(binarySearch)

          val ser = sfactory.newInstance()
          Utils.serializeViaNestedStream(out, ser) { stream =>
            stream.writeObject(scala.reflect.classTag[Array[K]])
            stream.writeObject(rangeBounds)
          }
      }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    Utils.tryOrIOException {
      needInit = true
      val sfactory = SparkEnv.get.serializer
      sfactory match {
        case js: JavaSerializer => in.defaultReadObject()
        case _ =>
          skewCount = in.readInt()
          skewPartitions = in.readInt()
          if (skewCount > 0) {
            skewIndexes = in.readObject().asInstanceOf[Array[Int]]
            skewWeights = in.readObject().asInstanceOf[Array[Int]]
          }
          ordering = in.readObject().asInstanceOf[Ordering[K]]
          binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

          val ser = sfactory.newInstance()
          Utils.deserializeViaNestedStream(in, ser) { ds =>
            implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
            rangeBounds = ds.readObject[Array[K]]()
          }
      }
    }
  }
}

class DataSkewPartitioner(originPart: Int, previousPart: Int, skewWeight: Int) {
  var index = 0

  def nextPartition(): Int = {
    if (index == 0) {
      index = index + 1
      originPart
    } else {
      val newPartition = previousPart + index
      index = index + 1
      if (index == skewWeight) {
        index = 0
      }
      newPartition
    }
  }
}
