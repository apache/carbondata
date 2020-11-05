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

package org.apache.spark.sql.parser

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.{CarbonAntlrSqlVisitor, CarbonMergeIntoSQLCommand}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.command.AtomicRunnableCommand

class CarbonAntlrParser(sparkParser: ParserInterface) {

  def parse(sqlText: String): AtomicRunnableCommand = {

    if (!sqlText.trim.toLowerCase.startsWith("merge")) {
      throw new UnsupportedOperationException(
        "Antlr SQL Parser will only deal with Merge Into SQL Command")
    }
    val visitor = new CarbonAntlrSqlVisitor(sparkParser)
    val lexer = new CarbonSqlBaseLexer(CharStreams.fromString(sqlText))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CarbonSqlBaseParser(tokenStream)
    val mergeInto = visitor.visitMergeIntoCarbonTable(parser.mergeInto)
    // In this place check the mergeInto Map for update *
    CarbonMergeIntoSQLCommand(mergeInto)
  }
}
