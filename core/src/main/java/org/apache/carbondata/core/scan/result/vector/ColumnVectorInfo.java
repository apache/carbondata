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
package org.apache.carbondata.core.scan.result.vector;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;

public class ColumnVectorInfo implements Comparable<ColumnVectorInfo> {
  public int offset;
  public int size;
  public CarbonColumnVector vector;
  public int vectorOffset;
  public QueryDimension dimension;
  public QueryMeasure measure;
  public int ordinal;
  public DirectDictionaryGenerator directDictionaryGenerator;
  public MeasureDataVectorProcessor.MeasureVectorFiller measureVectorFiller;
  public GenericQueryType genericQueryType;

  @Override public int compareTo(ColumnVectorInfo o) {
    return ordinal - o.ordinal;
  }
}
