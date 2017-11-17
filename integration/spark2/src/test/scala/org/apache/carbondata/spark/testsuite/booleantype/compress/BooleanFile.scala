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

package org.apache.carbondata.spark.testsuite.booleantype.compress

import java.io.{File, PrintWriter}

import scala.util.Random

object BooleanFile {

  val randomNumber = 10000
  def createBooleanFileRandom(path: String, totalLines: Int, rate: Double): Boolean = {
    try {
      val write = new PrintWriter(path)
      var d: Double = 0.0
      val random = new Random()
      for (i <- 0 until totalLines) {
        val eachNum = random.nextInt(randomNumber)
        var flag: Boolean = true
        if (eachNum >= randomNumber * rate) {
          flag = false
        }
        write.println(flag)
        d = d + 1
      }

      write.close()
    } catch {
      case _: Exception => assert(false)
    }
    return true
  }

  def deleteFile(path: String): Boolean = {
    try {
      val file = new File(path)
      file.delete()
    } catch {
      case _: Exception => assert(false)
    }
    return true
  }

}
