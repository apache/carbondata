package org.carbondata.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.metadata.ValueEncoderMeta;
import org.carbondata.format.*;

/**
 * Util class to convert to thrift metdata classes
 */
public class CarbonMetadataUtil {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonMetadataUtil.class.getName());

  /**
   * It converts list of LeafNodeInfoColumnar to FileFooter thrift objects
   *
   * @param infoList
   * @param numCols
   * @param cardinalities
   * @return FileFooter
   */
  public static FileFooter convertFileFooter(List<LeafNodeInfoColumnar> infoList, int numCols,
      int[] cardinalities, List<ColumnSchema> columnSchemaList) throws IOException {

    SegmentInfo segmentInfo = new SegmentInfo();
    segmentInfo.setNum_cols(numCols);
    segmentInfo.setColumn_cardinalities(CarbonUtil.convertToIntegerList(cardinalities));

    FileFooter footer = new FileFooter();
    footer.setNum_rows(getTotalNumberOfRows(infoList));
    footer.setSegment_info(segmentInfo);
    for (LeafNodeInfoColumnar info : infoList) {
      footer.addToBlocklet_index_list(getLeafNodeIndex(info));
    }
    footer.setTable_columns(columnSchemaList);
    for (LeafNodeInfoColumnar info : infoList) {
      footer.addToBlocklet_info_list(getLeafNodeInfo(info));
    }
    return footer;
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

  private static BlockletIndex getLeafNodeIndex(LeafNodeInfoColumnar info) {

    BlockletMinMaxIndex leafNodeMinMaxIndex = new BlockletMinMaxIndex();
    for (byte[] max : info.getColumnMaxData()) {
      leafNodeMinMaxIndex.addToMax_values(ByteBuffer.wrap(max));
    }
    for (byte[] min : info.getColumnMinData()) {
      leafNodeMinMaxIndex.addToMin_values(ByteBuffer.wrap(min));
    }
    BlockletBTreeIndex leafNodeBTreeIndex = new BlockletBTreeIndex();
    leafNodeBTreeIndex.setStart_key(info.getStartKey());
    leafNodeBTreeIndex.setEnd_key(info.getEndKey());

    BlockletIndex leafNodeIndex = new BlockletIndex();
    leafNodeIndex.setMin_max_index(leafNodeMinMaxIndex);
    leafNodeIndex.setB_tree_index(leafNodeBTreeIndex);
    return leafNodeIndex;
  }

  private static BlockletInfo getLeafNodeInfo(LeafNodeInfoColumnar leafNodeInfoColumnar)
      throws IOException {

    BlockletInfo leafNodeInfo = new BlockletInfo();
    leafNodeInfo.setNum_rows(leafNodeInfoColumnar.getNumberOfKeys());

    List<DataChunk> colDataChunks = new ArrayList<DataChunk>();
    leafNodeInfoColumnar.getKeyLengths();
    int j = 0;
    int aggregateIndex = 0;
    boolean[] isSortedKeyColumn = leafNodeInfoColumnar.getIsSortedKeyColumn();
    boolean[] aggKeyBlock = leafNodeInfoColumnar.getAggKeyBlock();
    for (int i = 0; i < leafNodeInfoColumnar.getKeyLengths().length; i++) {
      DataChunk dataChunk = new DataChunk();
      dataChunk.setChunk_meta(getChunkCompressionMeta());
      List<Encoding> encodings = new ArrayList<Encoding>();
      encodings.add(Encoding.DICTIONARY);
      //TODO : Need to find how to set it.
      dataChunk.setRow_chunk(false);
      //TODO : Once schema PR is merged and information needs to be passed here.
      dataChunk.setColumn_ids(new ArrayList<Integer>());
      dataChunk.setData_page_length(leafNodeInfoColumnar.getKeyLengths()[i]);
      dataChunk.setData_page_offset(leafNodeInfoColumnar.getKeyOffSets()[i]);
      if (aggKeyBlock[i]) {
        dataChunk.setRle_page_offset(leafNodeInfoColumnar.getDataIndexMapOffsets()[aggregateIndex]);
        dataChunk.setRle_page_length(leafNodeInfoColumnar.getDataIndexMapLength()[aggregateIndex]);
        encodings.add(Encoding.RLE);
        aggregateIndex++;
      }
      dataChunk
          .setSort_state(isSortedKeyColumn[i] ? SortState.SORT_EXPLICIT : SortState.SORT_NATIVE);

      if (!isSortedKeyColumn[i]) {
        dataChunk.setRowid_page_offset(leafNodeInfoColumnar.getKeyBlockIndexOffSets()[j]);
        dataChunk.setRowid_page_length(leafNodeInfoColumnar.getKeyBlockIndexLength()[j]);
        encodings.add(Encoding.INVERTED_INDEX);
        j++;
      }

      //TODO : Right now the encodings are happening at runtime. change as per this encoders.
      dataChunk.setEncoders(encodings);

      colDataChunks.add(dataChunk);
    }

    for (int i = 0; i < leafNodeInfoColumnar.getMeasureLength().length; i++) {
      DataChunk dataChunk = new DataChunk();
      dataChunk.setChunk_meta(getChunkCompressionMeta());
      dataChunk.setRow_chunk(false);
      //TODO : Once schema PR is merged and information needs to be passed here.
      dataChunk.setColumn_ids(new ArrayList<Integer>());
      dataChunk.setData_page_length(leafNodeInfoColumnar.getMeasureLength()[i]);
      dataChunk.setData_page_offset(leafNodeInfoColumnar.getMeasureOffset()[i]);
      //TODO : Right now the encodings are happening at runtime. change as per this encoders.
      List<Encoding> encodings = new ArrayList<Encoding>();
      encodings.add(Encoding.DELTA);
      dataChunk.setEncoders(encodings);
      //TODO writing dummy presence meta need to set actual presence
      //meta
      PresenceMeta presenceMeta = new PresenceMeta();
      presenceMeta.setPresent_bit_streamIsSet(true);
      presenceMeta.setPresent_bit_stream(new BitSet().toByteArray());
      dataChunk.setPresence(presenceMeta);
      //TODO : PresenceMeta needs to be implemented and set here
      // dataChunk.setPresence(new PresenceMeta());
      //TODO : Need to write ValueCompression meta here.
      List<ByteBuffer> encoderMetaList = new ArrayList<ByteBuffer>();
      encoderMetaList.add(ByteBuffer.wrap(serializeEncoderMeta(
          createValueEncoderMeta(leafNodeInfoColumnar.getCompressionModel(), i))));
      dataChunk.setEncoder_meta(encoderMetaList);
      colDataChunks.add(dataChunk);
    }
    leafNodeInfo.setColumn_data_chunks(colDataChunks);

    return leafNodeInfo;
  }

  private static byte[] serializeEncoderMeta(ValueEncoderMeta encoderMeta) throws IOException {
    // TODO : should remove the unnecessary fields.
    ByteArrayOutputStream aos = new ByteArrayOutputStream();
    ObjectOutputStream objStream = new ObjectOutputStream(aos);
    objStream.writeObject(encoderMeta);
    objStream.close();
    return aos.toByteArray();
  }

  private static ValueEncoderMeta createValueEncoderMeta(ValueCompressionModel compressionModel,
      int index) {
    ValueEncoderMeta encoderMeta = new ValueEncoderMeta();
    encoderMeta.setMaxValue(compressionModel.getMaxValue()[index]);
    encoderMeta.setMinValue(compressionModel.getMinValue()[index]);
    encoderMeta.setDataTypeSelected(compressionModel.getDataTypeSelected()[index]);
    encoderMeta.setDecimal(compressionModel.getDecimal()[index]);
    encoderMeta.setType(compressionModel.getType()[index]);
    encoderMeta.setUniqueValue(compressionModel.getUniqueValue()[index]);
    return encoderMeta;
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
   * It converts FileFooter thrift object to list of LeafNodeInfoColumnar objects
   *
   * @param footer
   * @return
   */
  public static List<LeafNodeInfoColumnar> convertLeafNodeInfo(FileFooter footer)
      throws IOException {
    List<LeafNodeInfoColumnar> listOfNodeInfo =
        new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (BlockletInfo leafNodeInfo : footer.getBlocklet_info_list()) {
      LeafNodeInfoColumnar leafNodeInfoColumnar = new LeafNodeInfoColumnar();
      leafNodeInfoColumnar.setNumberOfKeys(leafNodeInfo.getNum_rows());
      List<DataChunk> columnChunks = leafNodeInfo.getColumn_data_chunks();
      List<DataChunk> dictChunks = new ArrayList<DataChunk>();
      List<DataChunk> nonDictColChunks = new ArrayList<DataChunk>();
      for (DataChunk dataChunk : columnChunks) {
        if (dataChunk.getEncoders().get(0).equals(Encoding.DICTIONARY)) {
          dictChunks.add(dataChunk);
        } else {
          nonDictColChunks.add(dataChunk);
        }
      }
      int[] keyLengths = new int[dictChunks.size()];
      long[] keyOffSets = new long[dictChunks.size()];
      long[] keyBlockIndexOffsets = new long[dictChunks.size()];
      int[] keyBlockIndexLens = new int[dictChunks.size()];
      long[] indexMapOffsets = new long[dictChunks.size()];
      int[] indexMapLens = new int[dictChunks.size()];
      boolean[] sortState = new boolean[dictChunks.size()];
      int i = 0;
      for (DataChunk dataChunk : dictChunks) {
        keyLengths[i] = dataChunk.getData_page_length();
        keyOffSets[i] = dataChunk.getData_page_offset();
        keyBlockIndexOffsets[i] = dataChunk.getRowid_page_offset();
        keyBlockIndexLens[i] = dataChunk.getRowid_page_length();
        indexMapOffsets[i] = dataChunk.getRle_page_offset();
        indexMapLens[i] = dataChunk.getRle_page_length();
        sortState[i] = dataChunk.getSort_state().equals(SortState.SORT_EXPLICIT) ? true : false;
        i++;
      }
      leafNodeInfoColumnar.setKeyLengths(keyLengths);
      leafNodeInfoColumnar.setKeyOffSets(keyOffSets);
      leafNodeInfoColumnar.setKeyBlockIndexOffSets(keyBlockIndexOffsets);
      leafNodeInfoColumnar.setKeyBlockIndexLength(keyBlockIndexLens);
      leafNodeInfoColumnar.setDataIndexMapOffsets(indexMapOffsets);
      leafNodeInfoColumnar.setDataIndexMapLength(indexMapLens);
      leafNodeInfoColumnar.setIsSortedKeyColumn(sortState);

      int[] msrLens = new int[nonDictColChunks.size()];
      long[] msrOffsets = new long[nonDictColChunks.size()];
      ValueEncoderMeta[] encoderMetas = new ValueEncoderMeta[nonDictColChunks.size()];
      i = 0;
      for (DataChunk msrChunk : nonDictColChunks) {
        msrLens[i] = msrChunk.getData_page_length();
        msrOffsets[i] = msrChunk.getData_page_offset();
        encoderMetas[i] = deserializeValueEncoderMeta(msrChunk.getEncoder_meta().get(0));
        i++;
      }
      leafNodeInfoColumnar.setMeasureLength(msrLens);
      leafNodeInfoColumnar.setMeasureOffset(msrOffsets);
      leafNodeInfoColumnar.setCompressionModel(getValueCompressionModel(encoderMetas));
      listOfNodeInfo.add(leafNodeInfoColumnar);
    }

    setLeafNodeIndex(footer, listOfNodeInfo);
    return listOfNodeInfo;
  }

  private static ValueEncoderMeta deserializeValueEncoderMeta(ByteBuffer byteBuffer)
      throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(byteBuffer.array());
    ObjectInputStream objStream = new ObjectInputStream(bis);
    ValueEncoderMeta encoderMeta = null;
    try {
      encoderMeta = (ValueEncoderMeta) objStream.readObject();
    } catch (ClassNotFoundException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "Error while reading ValueEncoderMeta",
          e);
    }
    return encoderMeta;

  }

  private static ValueCompressionModel getValueCompressionModel(ValueEncoderMeta[] encoderMetas) {
    Object[] maxValue = new Object[encoderMetas.length];
    Object[] minValue = new Object[encoderMetas.length];
    int[] decimalLength = new int[encoderMetas.length];
    Object[] uniqueValue = new Object[encoderMetas.length];
    char[] aggType = new char[encoderMetas.length];
    byte[] dataTypeSelected = new byte[encoderMetas.length];
    for (int i = 0; i < encoderMetas.length; i++) {
      maxValue[i] = encoderMetas[i].getMaxValue();
      minValue[i] = encoderMetas[i].getMinValue();
      decimalLength[i] = encoderMetas[i].getDecimal();
      uniqueValue[i] = encoderMetas[i].getUniqueValue();
      aggType[i] = encoderMetas[i].getType();
      dataTypeSelected[i] = encoderMetas[i].getDataTypeSelected();
    }
    return ValueCompressionUtil
        .getValueCompressionModel(maxValue, minValue, decimalLength, uniqueValue, aggType,
            dataTypeSelected);
  }

  private static void setLeafNodeIndex(FileFooter footer,
      List<LeafNodeInfoColumnar> listOfNodeInfo) {
    List<BlockletIndex> blockletIndexList = footer.getBlocklet_index_list();
    for (int i = 0; i < blockletIndexList.size(); i++) {
      BlockletBTreeIndex bTreeIndexList = blockletIndexList.get(i).getB_tree_index();
      BlockletMinMaxIndex minMaxIndexList = blockletIndexList.get(i).getMin_max_index();

      listOfNodeInfo.get(i).setStartKey(bTreeIndexList.getStart_key());
      listOfNodeInfo.get(i).setEndKey(bTreeIndexList.getEnd_key());
      byte[][] min = new byte[minMaxIndexList.getMin_values().size()][];
      byte[][] max = new byte[minMaxIndexList.getMax_values().size()][];
      for (int j = 0; j < minMaxIndexList.getMax_valuesSize(); j++) {
        min[j] = minMaxIndexList.getMin_values().get(j).array();
        max[j] = minMaxIndexList.getMax_values().get(j).array();
      }

      //      byte[][] min = new byte[minMaxIndexList.getMin_values().size()][];
      //      List<ByteBuffer> minValues = minMaxIndexList.getMin_values();
      //      for (int j = 0; j < minValues.size(); j++) {
      //        min[j] = minValues.get(j).array();
      //      }
      //      listOfNodeInfo.get(i).setColumnMinData(min);
      //
      //      byte[][] max = new byte[minMaxIndexList.getMax_values().size()][];
      //      List<ByteBuffer> maxValues = minMaxIndexList.getMax_values();
      //      for (int j = 0; j < maxValues.size(); j++) {
      //        max[j] = maxValues.get(j).array();
      //    }
      listOfNodeInfo.get(i).setColumnMaxData(max);
    }
  }

}
