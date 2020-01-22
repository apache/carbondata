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

package org.apache.carbondata.spark.rdd

import java.util.regex.Pattern

import org.apache.spark.sql.Row

case class DataFormat(delimiters: Array[String],
    var delimiterIndex: Int,
    patterns: Array[Pattern]) extends Serializable {
  self =>
  def getSplits(input: String): Array[String] = {
    // -1 in case after splitting the last column is empty, the surrogate key ahs to be generated
    // for empty value too
    patterns(delimiterIndex).split(input, -1)
  }

  def cloneAndIncreaseIndex: DataFormat = {
    DataFormat(delimiters, Math.min(delimiterIndex + 1, delimiters.length - 1), patterns)
  }
}

class StringArrayRow(var values: Array[String]) extends Row {

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def getString(i: Int): String = values(i)

  private def reset(): Unit = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = null
      i = i + 1
    }
  }

  override def copy(): Row = {
    val tmpValues = new Array[String](values.length)
    System.arraycopy(values, 0, tmpValues, 0, values.length)
    new StringArrayRow(tmpValues)
  }

  def setValues(values: Array[String]): StringArrayRow = {
    reset()
    if (values != null) {
      val minLength = Math.min(this.values.length, values.length)
      System.arraycopy(values, 0, this.values, 0, minLength)
    }
    this
  }
}
