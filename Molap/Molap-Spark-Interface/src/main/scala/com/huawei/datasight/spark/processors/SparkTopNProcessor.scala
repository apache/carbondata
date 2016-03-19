/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
  *
  */
package com.huawei.datasight.spark.processors

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.huawei.unibi.molap.engine.executer.impl.topn.TopNModel
import scala.collection.immutable.List
import java.util.ArrayList
import scala.collection.mutable.MutableList
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.unibi.molap.engine.scanner.impl.MolapKeyValueGroup
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator
import com.huawei.unibi.molap.engine.scanner.impl.MolapKeyValueTopNGroup
import com.huawei.unibi.molap.engine.executer.impl.topn.TopNModel.MolapTopNType

/**
  * @author R00900208
  *
  */
class SparkTopNProcessor {

  def process(rdd: RDD[(MolapKey, MolapValue)], count: Int, dimIndex: Int, msrIndex: Int, topNType: MolapTopNType): Array[(MolapKey, MolapValue)] = {
    if (false) {
      val m = rdd.map { key => (key._1, key._2) }.reduceByKey((v1, v2) => v1.merge(v2)).map({ key => key.swap }).top(count).map(key => key.swap)
      return m
    }
    else if (true) {
      //First create map with one group
      val s = rdd.map { key =>

        val value: MolapValue = new MolapKeyValueGroup(Array[MeasureAggregator] {
          key._2.getValues()(msrIndex)
        }.clone)
        value.addGroup(key._1, key._2);
        (key._1.getSubKey(dimIndex + 1), value)
      }

        .reduceByKey((v1, v2) => v1.mergeKeyVal(v2))

        .map { key =>

          val value: MolapValue = new MolapKeyValueTopNGroup(key._2.getValues(), key._2, count, topNType)
          (key._1.getSubKey(dimIndex), value)
        }

        .reduceByKey((v1, v2) => v1.mergeKeyVal(v2))

        .flatMap { key =>
          val value = key._2.asInstanceOf[MolapKeyValueTopNGroup]
          val iter = value.getGroups().iterator()
          val list = new MutableList[(MolapKey, MolapValue)]()
          while (iter.hasNext()) {
            val next = iter.next()
            val keys = next.getKeys()
            val values = next.getAllValues();
            for (i <- 0 until keys.size()) {
              list += ((keys.get(i), values.get(i)))
            }

          }

          list
        }
        .collect
      return s
    }
    return null;
  }


}