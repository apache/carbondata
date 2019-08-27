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

package org.apache.carbondata.streaming.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * Stream parser interface
 */
public interface CarbonStreamParser {

  String CARBON_STREAM_PARSER = "carbon.stream.parser";

  String CARBON_STREAM_PARSER_CSV =
      "org.apache.carbondata.streaming.parser.CSVStreamParserImp";

  String CARBON_STREAM_PARSER_ROW_PARSER =
      "org.apache.carbondata.streaming.parser.RowStreamParserImp";

  String CARBON_STREAM_PARSER_DEFAULT = CARBON_STREAM_PARSER_ROW_PARSER;

  void initialize(Configuration configuration,
      StructType structType, boolean[] isVarcharTypeMapping);

  Object[] parserRow(InternalRow value);

  void close();

}
