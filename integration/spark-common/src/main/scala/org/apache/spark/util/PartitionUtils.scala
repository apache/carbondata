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
package org.apache.spark.util

import scala.collection.mutable.ListBuffer

object PartitionUtils {

  def getListInfo(originListInfo: String): List[List[String]] = {
    var listInfo = ListBuffer[List[String]]()
    var templist = ListBuffer[String]()
    val arr = originListInfo.split(",")
      .map(_.trim())
    var groupEnd = true
    val iter = arr.iterator
    while (iter.hasNext) {
      val value = iter.next()
      if (value.startsWith("(")) {
        templist += value.replace("(", "").trim()
        groupEnd = false
      } else if (value.endsWith(")")) {
        templist += value.replace(")", "").trim()
        listInfo += templist.toList
        templist.clear()
        groupEnd = true
      } else {
        if (groupEnd) {
          templist += value
          listInfo += templist.toList
          templist.clear()
        } else {
          templist += value
        }
      }
    }
    listInfo.toList
  }
}
