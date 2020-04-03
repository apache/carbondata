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

package org.apache.carbondata.core.index.dev.fgindex;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;

/**
 *  Factory for {@link FineGrainIndex}
 *
 *  1. Any filter query which hits the table with index will call prune method of FGindex.
 *  2. The prune method of FGindex return list FineGrainBlocklet , these blocklets contain the
 *     information of block, blocklet, page and rowids information as well.
 *  3. The pruned blocklets are internally wriitten to file and returns only the block ,
 *    blocklet and filepath information as part of Splits.
 *  4. Based on the splits scanrdd schedule the tasks.
 *  5. In filterscanner we check the indexwriterpath from split and reNoteads the
 *     bitset if exists. And pass this bitset as input to it.
 */
@InterfaceAudience.Developer("Index")
@InterfaceStability.Evolving
public abstract class FineGrainIndexFactory extends IndexFactory<FineGrainIndex> {

  public FineGrainIndexFactory(CarbonTable carbonTable, IndexSchema indexSchema) {
    super(carbonTable, indexSchema);
  }

  @Override
  public IndexLevel getIndexLevel() {
    return IndexLevel.FG;
  }
}
