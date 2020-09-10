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
package org.apache.carbondata.presto.server

import io.prestosql.jdbc.PrestoArray

object PrestoTestUtil {

  // this method depends on prestosql jdbc PrestoArray class
  def validateArrayOfPrimitiveTypeData(actualResult: List[Map[String, Any]],
      longChar: String): Unit = {
    for (row <- 0 to 1) {
      val column1 = actualResult(row)("stringfield")
      if (column1 == "row1") {
        val column2 = actualResult(row)("arraybyte")
          .asInstanceOf[PrestoArray]
          .getArray()
          .asInstanceOf[Array[Object]]
        val column3 = actualResult(row)("arrayshort")
          .asInstanceOf[PrestoArray]
          .getArray()
          .asInstanceOf[Array[Object]]
        val column4 = actualResult(row)("arrayint")
          .asInstanceOf[PrestoArray]
          .getArray()
          .asInstanceOf[Array[Object]]
        assert(column2(0) == null)
        assert(column3(0) == null)
        assert(column4(0) == null)
      } else if (column1 == "row2") {
        val column2 = actualResult(row)("arrayint")
          .asInstanceOf[PrestoArray]
          .getArray()
          .asInstanceOf[Array[Object]]
        if (column2.sameElements(Array(4))) {
          val column3 = actualResult(row)("arraybyte")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column4 = actualResult(row)("arrayshort")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column5 = actualResult(row)("arraylong")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column6 = actualResult(row)("arrayfloat")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column7 = actualResult(row)("arraydouble")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column8 = actualResult(row)("arraybinary")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column9 = actualResult(row)("arraydate")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column10 = actualResult(row)("arraytimestamp")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column11 = actualResult(row)("arrayboolean")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column12 = actualResult(row)("arrayvarchar")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column13 = actualResult(row)("arraydecimal")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]
          val column14 = actualResult(row)("arraystring")
            .asInstanceOf[PrestoArray]
            .getArray()
            .asInstanceOf[Array[Object]]

          assert(column3.sameElements(Array(3, 5, 4)))
          assert(column4.sameElements(Array(4, 5, 6)))
          assert(column5.sameElements(Array(2L, 59999999L, 99999999999L)))
          assert(column6.sameElements(Array(5.4646f, 5.55f, 0.055f)))
          assert(column7.sameElements(Array(5.46464646464, 5.55, 0.055)))
          assert(column8(0).asInstanceOf[Array[Byte]].length == 118198)
          assert(column9.sameElements(Array("2019-03-02", "2020-03-02", "2021-04-02")))
          assert(column10.sameElements(Array("2019-02-12 03:03:34.000",
            "2020-02-12 03:03:34.000",
            "2021-03-12 03:03:34.000")))
          assert(column11.sameElements(Array(true, false)))
          assert(column12.sameElements(Array(longChar)))
          assert(column13.sameElements(Array("999.23", "0.12")))
          assert(column14.sameElements(Array("japan", "china", "iceland")))
        }
      }
    }
  }
}
