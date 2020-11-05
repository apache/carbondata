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

import carbonSql.codeGen.{CarbonSqlBaseLexer, CarbonSqlBaseParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.{AntlrSqlVisitor, MergeIntoSQLCommand}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.command.AtomicRunnableCommand

class CarbonAntlrParser(sparkParser: ParserInterface) {

  def parse(sqlText: String): AtomicRunnableCommand = {

    if (!(sqlText.startsWith("MERGE") || sqlText.startsWith("merge"))) {
      throw new UnsupportedOperationException(
        "Antlr SQL Parser will only deal with Merge Into SQL Command")
    }
    // Todo DO NOT NEW OBJECTS HERE
    val visitor = new AntlrSqlVisitor(sparkParser)
    val lexer = new CarbonSqlBaseLexer(CharStreams.fromString(sqlText))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CarbonSqlBaseParser(tokenStream)
    val mergeInto = visitor.visitMergeInto(parser.mergeInto)
    // In this place check the mergeInto Map for update *
    MergeIntoSQLCommand(mergeInto)
  }
}
