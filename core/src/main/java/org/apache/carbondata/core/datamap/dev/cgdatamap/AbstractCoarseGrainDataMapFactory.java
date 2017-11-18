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
package org.apache.carbondata.core.datamap.dev.cgdatamap;

import org.apache.carbondata.core.datamap.DataMapType;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;

/**
 *  1. Any filter query which hits the table with datamap will call prune method of CGdatamap.
 *  2. The prune method of CGDatamap return list Blocklet , these blocklets contain the
 *     information of block and blocklet.
 *  3. Based on the splits scanrdd schedule the tasks.
 */
public abstract class AbstractCoarseGrainDataMapFactory
    implements DataMapFactory<AbstractCoarseGrainDataMap> {

  @Override public DataMapType getDataMapType() {
    return DataMapType.CG;
  }
}
