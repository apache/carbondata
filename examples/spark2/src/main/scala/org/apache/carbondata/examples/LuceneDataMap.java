package org.apache.carbondata.examples;/*
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


import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Datamap implementation for min max blocklet.
 */
public class LuceneDataMap implements DataMap {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LuceneDataMap.class.getName());

    public static final String NAME = "clustered.lucene";

    private String filePath;

    @Override public void init(String filePath) throws MemoryException, IOException {
        this.filePath = filePath;
    }

    /**
     * Block Prunning logic for Min Max DataMap.
     *
     * @param filterExps
     * @return
     */
    @Override public List<Blocklet> prune(FilterResolverIntf filterExps) {

        Set<Blocklet> blocklets = new HashSet<>();
        Map<String, String> blockletPath = new HashMap<>();
        try {
            FSDirectory index = FSDirectory.open(Paths.get(filePath));
            IndexReader indexReader = DirectoryReader.open(index);
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);

            Term t = new Term("Content", "b");
            org.apache.lucene.search.Query query = new TermQuery(t);

            TopDocs doc = indexSearcher.search(query, 10);
            LOGGER.debug("Total Hits are :: " + doc.totalHits);

            ScoreDoc[] hits = doc.scoreDocs;
            for (int i = 0; i < hits.length; i ++) {
                Document document = indexSearcher.doc(hits[i].doc);
                LOGGER.debug(
                    "The values are " + document.get("BlockletPath") + " " + document.get("Content")
                        + " " + document.get("BlockletId"));
                if (blockletPath.get(document.get("BlockletPath")) == null) {
                    blocklets.add(new Blocklet(document.get("BlockletPath"), document.get("BlockletId")));
                    blockletPath.put(document.get("BlockletPath"), document.get("BlockletId"));
                }
            }
        } catch (IOException ex) {
            LOGGER.info("Error while searching the lucene Index");
        }
        List<Blocklet> blockletList = new ArrayList<>(blocklets);
        return blockletList;
    }

    @Override
    public void clear() {
    }
}