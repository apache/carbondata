package org.carbondata.core.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.format.*;

/**
 * Util class to convert to thrift metdata classes
 */
public class CarbonMetadataUtil {

    /**
     * It converts list of LeafNodeInfoColumnar to FileMeta thrift objects
     *
     * @param infoList
     * @param numCols
     * @param cardinalities
     * @return FileMeta
     */
    public static FileMeta convertFileMeta(List<LeafNodeInfoColumnar> infoList, int numCols,
            int[] cardinalities) {

        SegmentInfo segmentInfo = new SegmentInfo();
        segmentInfo.setNum_cols(numCols);
        segmentInfo.setColumn_cardinalities(CarbonUtil.convertToIntegerList(cardinalities));

        FileMeta fileMeta = new FileMeta();
        fileMeta.setNum_rows(getTotalNumberOfRows(infoList));
        fileMeta.setSegment_info(segmentInfo);
        fileMeta.setIndex(getLeafNodeIndex(infoList));
        //TODO: Need to set the schema here.
        fileMeta.setTable_columns(new ArrayList<ColumnSchema>());
        for (LeafNodeInfoColumnar info : infoList) {
            fileMeta.addToLeaf_node_info(getLeafNodeInfo(info));
        }
        return fileMeta;
    }

    /**
     * Get total number of rows for the file.
     *
     * @param infoList
     * @return
     */
    private static long getTotalNumberOfRows(List<LeafNodeInfoColumnar> infoList) {
        long numberOfRows = 0;
        for (LeafNodeInfoColumnar info : infoList) {
            numberOfRows += info.getNumberOfKeys();
        }
        return numberOfRows;
    }

    private static LeafNodeIndex getLeafNodeIndex(List<LeafNodeInfoColumnar> infoList) {

        List<LeafNodeMinMaxIndex> leafNodeMinMaxIndexes = new ArrayList<LeafNodeMinMaxIndex>();
        List<LeafNodeBTreeIndex> leafNodeBTreeIndexes = new ArrayList<LeafNodeBTreeIndex>();

        for (LeafNodeInfoColumnar info : infoList) {
            LeafNodeMinMaxIndex leafNodeMinMaxIndex = new LeafNodeMinMaxIndex();
            //TODO: Need to seperate minmax and set.
            for (byte[] minMax : info.getColumnMinMaxData()) {
                leafNodeMinMaxIndex.addToMax_values(ByteBuffer.wrap(minMax));
                leafNodeMinMaxIndex.addToMin_values(ByteBuffer.wrap(minMax));
            }
            leafNodeMinMaxIndexes.add(leafNodeMinMaxIndex);

            LeafNodeBTreeIndex leafNodeBTreeIndex = new LeafNodeBTreeIndex();
            leafNodeBTreeIndex.setStart_key(info.getStartKey());
            leafNodeBTreeIndex.setEnd_key(info.getEndKey());
            leafNodeBTreeIndexes.add(leafNodeBTreeIndex);
        }

        LeafNodeIndex leafNodeIndex = new LeafNodeIndex();
        leafNodeIndex.setMin_max_index(leafNodeMinMaxIndexes);
        leafNodeIndex.setB_tree_index(leafNodeBTreeIndexes);
        return leafNodeIndex;
    }

    private static LeafNodeInfo getLeafNodeInfo(LeafNodeInfoColumnar leafNodeInfoColumnar) {

        LeafNodeInfo leafNodeInfo = new LeafNodeInfo();
        leafNodeInfo.setNum_rows(leafNodeInfoColumnar.getNumberOfKeys());

        List<DataChunk> dimDataChunks = new ArrayList<DataChunk>();
        List<DataChunk> msrDataChunks = new ArrayList<DataChunk>();
        leafNodeInfoColumnar.getKeyLengths();
        int j = 0;
        for (int i = 0; i < leafNodeInfoColumnar.getKeyLengths().length; i++) {
            DataChunk dataChunk = new DataChunk();
            dataChunk.setChunk_meta(getChunkCompressionMeta());
            boolean[] isSortedKeyColumn = leafNodeInfoColumnar.getIsSortedKeyColumn();
            //TODO : Need to find how to set it.
            dataChunk.setIs_row_chunk(false);
            //TODO : Once schema PR is merged and information needs to be passed here.
            dataChunk.setColumn_ids(new ArrayList<Integer>());
            dataChunk.setData_page_length(leafNodeInfoColumnar.getKeyLengths()[i]);
            dataChunk.setData_page_offset(leafNodeInfoColumnar.getKeyOffSets()[i]);
            dataChunk.setRle_page_offset(leafNodeInfoColumnar.getDataIndexMapOffsets()[i]);
            dataChunk.setRle_page_length(leafNodeInfoColumnar.getDataIndexMapLength()[i]);
            dataChunk.setSort_state(
                    isSortedKeyColumn[i] ? SortState.SORT_EXPLICIT : SortState.SORT_NATIVE);

            if (!isSortedKeyColumn[i]) {
                dataChunk.setRowid_page_offset(leafNodeInfoColumnar.getKeyBlockIndexOffSets()[j]);
                dataChunk.setRowid_page_length(leafNodeInfoColumnar.getKeyBlockIndexLength()[j]);
                j++;
            }

            //TODO : Right now the encodings are happening at runtime. change as per this encoders.
            //dataChunk.setEncoders(new ArrayList<Encoding>());

            dimDataChunks.add(dataChunk);
        }

        for (int i = 0; i < leafNodeInfoColumnar.getMeasureLength().length; i++) {
            DataChunk dataChunk = new DataChunk();
            dataChunk.setChunk_meta(getChunkCompressionMeta());
            dataChunk.setIs_row_chunk(false);
            //TODO : Once schema PR is merged and information needs to be passed here.
            dataChunk.setColumn_ids(new ArrayList<Integer>());
            dataChunk.setData_page_length(leafNodeInfoColumnar.getMeasureLength()[i]);
            dataChunk.setData_page_offset(leafNodeInfoColumnar.getMeasureOffset()[i]);

            //TODO : PresenceMeta needs to be implemented and set here
            // dataChunk.setPresence(new PresenceMeta());
            //TODO : Need to write ValueCompression meta here.
            //dataChunk.setEncoder_meta()
            msrDataChunks.add(dataChunk);
        }
        leafNodeInfo.setDimension_chunks(dimDataChunks);
        leafNodeInfo.setMeasure_chunks(msrDataChunks);

        return leafNodeInfo;
    }

    /**
     * Right now it is set to default values. We may use this in future
     */
    private static ChunkCompressionMeta getChunkCompressionMeta() {
        ChunkCompressionMeta chunkCompressionMeta = new ChunkCompressionMeta();
        chunkCompressionMeta.setCompression_codec(CompressionCodec.SNAPPY);
        chunkCompressionMeta.setTotal_compressed_size(0);
        chunkCompressionMeta.setTotal_uncompressed_size(0);
        return chunkCompressionMeta;
    }

    /**
     * It converts FileMeta thrift object to list of LeafNodeInfoColumnar objects
     *
     * @param fileMeta
     * @return
     */
    public static List<LeafNodeInfoColumnar> convertLeafNodeInfo(FileMeta fileMeta) {
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

    private static void setLeafNodeIndex(FileMeta fileMeta,
            List<LeafNodeInfoColumnar> listOfNodeInfo) {
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

}
