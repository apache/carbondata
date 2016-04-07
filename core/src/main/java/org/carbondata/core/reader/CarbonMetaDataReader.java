/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TBase;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.format.*;

/**
 * Reads the metadata from fact file in org.carbondata.format.FileMeta thrift object
 */
public class CarbonMetaDataReader {

    //Fact file path
    private String filePath;

    //From which offset of file this metadata should be read
    private long offset;

    public CarbonMetaDataReader(String filePath, long offset) {

        this.filePath = filePath;
        this.offset = offset;
    }

    /**
     * It reads the metadata in FileMeta thrift object format.
     *
     * @return
     * @throws IOException
     */
    public FileMeta readMetaData() throws IOException {
        ThriftReader thriftReader = openThriftReader(filePath);
        thriftReader.open();
        //Set the offset from where it should read
        thriftReader.setReadOffset(offset);
        FileMeta fileMeta = (FileMeta) thriftReader.read();
        thriftReader.close();
        return fileMeta;
    }

    /**
     * It converts FileMeta thrift object to list of LeafNodeInfoColumnar objects
     *
     * @param fileMeta
     * @return
     */
    public List<LeafNodeInfoColumnar> convertLeafNodeInfo(FileMeta fileMeta) {
        List<LeafNodeInfoColumnar> listOfNodeInfo =
                new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        for (LeafNodeInfo leafNodeInfo : fileMeta.getLeaf_node_info()) {
            LeafNodeInfoColumnar leafNodeInfoColumnar = new LeafNodeInfoColumnar();
            leafNodeInfoColumnar.setNumberOfKeys(leafNodeInfo.getNum_rows());
            List<DataChunk> dimChunks = leafNodeInfo.getDimension_chunks();
            int[] keyLengths = new int[dimChunks.size()];
            long[] keyOffSets = new long[dimChunks.size()];
            long[] keyBlockIndexOffsets = new long[dimChunks.size()];
            int[] keyBlockIndexLens = new int[dimChunks.size()];
            long[] indexMapOffsets = new long[dimChunks.size()];
            int[] indexMapLens = new int[dimChunks.size()];
            boolean[] sortState = new boolean[dimChunks.size()];
            int i = 0;
            for (DataChunk dataChunk : dimChunks) {
                keyLengths[i] = dataChunk.getData_page_length();
                keyOffSets[i] = dataChunk.getData_page_offset();
                keyBlockIndexOffsets[i] = dataChunk.getRowid_page_offset();
                keyBlockIndexLens[i] = dataChunk.getRowid_page_length();
                indexMapOffsets[i] = dataChunk.getRle_page_offset();
                indexMapLens[i] = dataChunk.getRle_page_length();
                sortState[i] =
                        dataChunk.getSort_state().equals(SortState.SORT_EXPLICIT) ? true : false;
                i++;
            }
            leafNodeInfoColumnar.setKeyLengths(keyLengths);
            leafNodeInfoColumnar.setKeyOffSets(keyOffSets);
            leafNodeInfoColumnar.setKeyBlockIndexOffSets(keyBlockIndexOffsets);
            leafNodeInfoColumnar.setKeyBlockIndexLength(keyBlockIndexLens);
            leafNodeInfoColumnar.setDataIndexMapOffsets(indexMapOffsets);
            leafNodeInfoColumnar.setDataIndexMapLength(indexMapLens);
            leafNodeInfoColumnar.setIsSortedKeyColumn(sortState);

            List<DataChunk> msrChunks = leafNodeInfo.getMeasure_chunks();

            int[] msrLens = new int[msrChunks.size()];
            long[] msrOffsets = new long[msrChunks.size()];
            i = 0;
            for (DataChunk msrChunk : msrChunks) {
                msrLens[i] = msrChunk.getData_page_length();
                msrOffsets[i] = msrChunk.getData_page_offset();
                i++;
            }
            leafNodeInfoColumnar.setMeasureLength(msrLens);
            leafNodeInfoColumnar.setMeasureOffset(msrOffsets);

            listOfNodeInfo.add(leafNodeInfoColumnar);
        }

        setLeafNodeIndex(fileMeta, listOfNodeInfo);
        return listOfNodeInfo;
    }

    private void setLeafNodeIndex(FileMeta fileMeta, List<LeafNodeInfoColumnar> listOfNodeInfo) {
        LeafNodeIndex leafNodeIndex = fileMeta.getIndex();
        List<LeafNodeBTreeIndex> bTreeIndexes = leafNodeIndex.getB_tree_index();
        List<LeafNodeMinMaxIndex> min_max_indexes = leafNodeIndex.getMin_max_index();
        int i = 0;
        for (LeafNodeBTreeIndex bTreeIndex : bTreeIndexes) {
            listOfNodeInfo.get(i).setStartKey(bTreeIndex.getStart_key());
            listOfNodeInfo.get(i).setEndKey(bTreeIndex.getEnd_key());
            i++;
        }

        i = 0;
        for (LeafNodeMinMaxIndex minMaxIndex : min_max_indexes) {

            byte[][] minMax = new byte[minMaxIndex.getMax_values().size()][];
            int j = 0;
            for (ByteBuffer byteBuffer : minMaxIndex.getMax_values()) {
                minMax[j] = byteBuffer.array();
                j++;
            }
            listOfNodeInfo.get(i).setColumnMinMaxData(minMax);
            i++;
        }
    }

    /**
     * Open the thrift reader
     *
     * @param filePath
     * @return
     * @throws IOException
     */
    private ThriftReader openThriftReader(String filePath) throws IOException {

        ThriftReader thriftReader = new ThriftReader(filePath, new ThriftReader.TBaseCreator() {
            @Override
            public TBase create() {
                return new FileMeta();
            }
        });
        return thriftReader;
    }

}
