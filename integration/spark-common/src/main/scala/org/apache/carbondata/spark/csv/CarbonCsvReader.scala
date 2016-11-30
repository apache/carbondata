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

package com.databricks.spark.sql.readers

/**
 * Parser for parsing lines in bulk. Use this when efficiency is desired.
 *
 * @param iter iterator over lines in the file
 * @param fieldSep the delimiter used to separate fields in a line
 * @param lineSep the delimiter used to separate lines
 * @param quote character used to quote fields
 * @param escape character used to escape the quote character
 * @param ignoreLeadingSpace ignore white space before a field
 * @param ignoreTrailingSpace ignore white space after a field
 * @param headers headers for the columns
 * @param inputBufSize size of buffer to use for parsing input, tune for performance
 * @param maxCols maximum number of columns allowed, for safety against bad inputs
 */
class CarbonBulkCsvReader (iter: Iterator[String],
    split: Int,
    fieldSep: Char = ',',
    lineSep: String = "\n",
    quote: Char = '"',
    escape: Char = '\\',
    commentMarker: Char = '#',
    ignoreLeadingSpace: Boolean = true,
    ignoreTrailingSpace: Boolean = true,
    headers: Seq[String],
    inputBufSize: Int = 128,
    maxCols: Int = 20480)
  extends CsvReader(fieldSep,
      lineSep,
      quote,
      escape,
      commentMarker,
      ignoreLeadingSpace,
      ignoreTrailingSpace,
      headers,
      inputBufSize,
      maxCols)
    with Iterator[Array[String]] {

  private val reader = new CarbonStringIteratorReader(iter)
  parser.beginParsing(reader)
  private var nextRecord = parser.parseNext()

  /**
   * get the next parsed line.
   *
   * @return array of strings where each string is a field in the CSV record
   */
  def next: Array[String] = {
    val curRecord = nextRecord
    if(curRecord != null) {
      nextRecord = parser.parseNext()
    } else {
      throw new NoSuchElementException("next record is null")
    }
    curRecord
  }

  def hasNext: Boolean = nextRecord != null

}

/**
 * A Reader that "reads" from a sequence of lines. Spark's textFile method removes newlines at
 * end of each line Univocity parser requires a Reader that provides access to the data to be
 * parsed and needs the newlines to be present
 * @param iter iterator over RDD[String]
 */
private class CarbonStringIteratorReader(val iter: Iterator[String]) extends java.io.Reader {

  private var next: Long = 0
  private var length: Long = 0  // length of input so far
  private var start: Long = 0
  private var str: String = null   // current string from iter

  /**
   * fetch next string from iter, if done with current one
   * pretend there is a new line at the end of every string we get from from iter
   */
  private def refill(): Unit = {
    if (length == next) {
      if (iter.hasNext) {
        str = iter.next
        start = length
        // add a space to every line except the last one to store '\n'
        if (iter.hasNext) {
          length += (str.length + 1) // allowance for newline removed by SparkContext.textFile()
        } else {
          length += str.length
        }
      } else {
        str = null
      }
    }
  }

  /**
   * read the next character, if at end of string pretend there is a new line
   */
  override def read(): Int = {
    refill()
    if(next >= length) {
      -1
    } else {
      val cur = next - start
      next += 1
      if (cur == str.length) '\n' else str.charAt(cur.toInt)
    }
  }

  /**
   * read from str into cbuf
   */
  def read(cbuf: Array[Char], off: Int, len: Int): Int = {
    refill()
    var n = 0
    if ((off < 0) || (off > cbuf.length) || (len < 0) ||
      ((off + len) > cbuf.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException()
    } else if (len == 0) {
      n = 0
    } else {
      if (next >= length) {   // end of input
        n = -1
      } else {
        n = Math.min(length - next, len).toInt // lesser of amount of input available or buf size
        // add a '\n' to every line except the last one
        if (n == length - next && iter.hasNext) {
          str.getChars((next - start).toInt, (next - start + n - 1).toInt, cbuf, off)
          cbuf(off + n - 1) = '\n'
        } else {
          str.getChars((next - start).toInt, (next - start + n).toInt, cbuf, off)
        }
        next += n
        if (n < len) {
          val m = read(cbuf, off + n, len - n)  // have more space, fetch more input from iter
          if(m != -1) n += m
        }
      }
    }
    n
  }

  override def skip(ns: Long): Long = {
    throw new IllegalArgumentException("Skip not implemented")
  }

  override def ready: Boolean = {
    refill()
    true
  }

  override def markSupported: Boolean = false

  override def mark(readAheadLimit: Int): Unit = {
    throw new IllegalArgumentException("Mark not implemented")
  }

  override def reset(): Unit = {
    throw new IllegalArgumentException("Mark and hence reset not implemented")
  }

  def close(): Unit = { }
}
