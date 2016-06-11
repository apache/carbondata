package org.carbondata.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.metadata.BlockletInfoColumnar;
import org.carbondata.core.metadata.ValueEncoderMeta;
import org.carbondata.format.BlockletBTreeIndex;
import org.carbondata.format.BlockletIndex;
import org.carbondata.format.BlockletInfo;
import org.carbondata.format.BlockletMinMaxIndex;
import org.carbondata.format.ChunkCompressionMeta;
import org.carbondata.format.ColumnSchema;
import org.carbondata.format.CompressionCodec;
import org.carbondata.format.DataChunk;
import org.carbondata.format.Encoding;
import org.carbondata.format.FileFooter;
import org.carbondata.format.PresenceMeta;
import org.carbondata.format.SegmentInfo;
import org.carbondata.format.SortState;

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
   * It converts list of BlockletInfoColumnar to FileFooter thrift objects
   *
   * @param infoList
   * @param numCols
   * @param cardinalities
   * @return FileFooter
   */
  public static FileFooter convertFileFooter(List<BlockletInfoColumnar> infoList, int numCols,
      int[] cardinalities, List<ColumnSchema> columnSchemaList) throws IOException {

    SegmentInfo segmentInfo = new SegmentInfo();
    segmentInfo.setNum_cols(columnSchemaList.size());
    segmentInfo.setColumn_cardinalities(CarbonUtil.convertToIntegerList(cardinalities));

    FileFooter footer = new FileFooter();
    footer.setNum_rows(getTotalNumberOfRows(infoList));
    footer.setSegment_info(segmentInfo);
    for (BlockletInfoColumnar info : infoList) {
      footer.addToBlocklet_index_list(getBlockletIndex(info));
    }
    footer.setTable_columns(columnSchemaList);
    for (BlockletInfoColumnar info : infoList) {
      footer.addToBlocklet_info_list(getBlockletInfo(info, columnSchemaList));
    }
    return footer;
  }

  /**
   * Get total number of rows for the file.
   *
   * @param infoList
   * @return
   */
  private static long getTotalNumberOfRows(List<BlockletInfoColumnar> infoList) {
    long numberOfRows = 0;
    for (BlockletInfoColumnar info : infoList) {
      numberOfRows += info.getNumberOfKeys();
    }
    return numberOfRows;
  }

  private static BlockletIndex getBlockletIndex(BlockletInfoColumnar info) {

    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    for (byte[] max : info.getColumnMaxData()) {
      blockletMinMaxIndex.addToMax_values(ByteBuffer.wrap(max));
    }
    for (byte[] min : info.getColumnMinData()) {
      blockletMinMaxIndex.addToMin_values(ByteBuffer.wrap(min));
    }
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStart_key(info.getStartKey());
    blockletBTreeIndex.setEnd_key(info.getEndKey());

    BlockletIndex blockletIndex = new BlockletIndex();
    blockletIndex.setMin_max_index(blockletMinMaxIndex);
    blockletIndex.setB_tree_index(blockletBTreeIndex);
    return blockletIndex;
  }

  private static BlockletInfo getBlockletInfo(BlockletInfoColumnar blockletInfoColumnar,
      List<ColumnSchema> columnSchenma) throws IOException {

    BlockletInfo blockletInfo = new BlockletInfo();
    blockletInfo.setNum_rows(blockletInfoColumnar.getNumberOfKeys());

    List<DataChunk> colDataChunks = new ArrayList<DataChunk>();
    blockletInfoColumnar.getKeyLengths();
    int j = 0;
    int aggregateIndex = 0;
    boolean[] isSortedKeyColumn = blockletInfoColumnar.getIsSortedKeyColumn();
    boolean[] aggKeyBlock = blockletInfoColumnar.getAggKeyBlock();
    boolean[] colGrpblock = blockletInfoColumnar.getColGrpBlocks();
    for (int i = 0; i < blockletInfoColumnar.getKeyLengths().length; i++) {
      DataChunk dataChunk = new DataChunk();
      dataChunk.setChunk_meta(getChunkCompressionMeta());
      List<Encoding> encodings = new ArrayList<Encoding>();
      if (columnSchenma.get(i).encoders.contains(Encoding.DICTIONARY)) {
        encodings.add(Encoding.DICTIONARY);
      }
      if (columnSchenma.get(i).encoders.contains(Encoding.DIRECT_DICTIONARY)) {
        encodings.add(Encoding.DIRECT_DICTIONARY);
      }
      dataChunk.setRowMajor(colGrpblock[i]);
      //TODO : Once schema PR is merged and information needs to be passed here.
      dataChunk.setColumn_ids(new ArrayList<Integer>());
      dataChunk.setData_page_length(blockletInfoColumnar.getKeyLengths()[i]);
      dataChunk.setData_page_offset(blockletInfoColumnar.getKeyOffSets()[i]);
      if (aggKeyBlock[i]) {
        dataChunk.setRle_page_offset(blockletInfoColumnar.getDataIndexMapOffsets()[aggregateIndex]);
        dataChunk.setRle_page_length(blockletInfoColumnar.getDataIndexMapLength()[aggregateIndex]);
        encodings.add(Encoding.RLE);
        aggregateIndex++;
      }
      dataChunk
          .setSort_state(isSortedKeyColumn[i] ? SortState.SORT_EXPLICIT : SortState.SORT_NATIVE);

      if (!isSortedKeyColumn[i]) {
        dataChunk.setRowid_page_offset(blockletInfoColumnar.getKeyBlockIndexOffSets()[j]);
        dataChunk.setRowid_page_length(blockletInfoColumnar.getKeyBlockIndexLength()[j]);
        encodings.add(Encoding.INVERTED_INDEX);
        j++;
      }

      //TODO : Right now the encodings are happening at runtime. change as per this encoders.
      dataChunk.setEncoders(encodings);

      colDataChunks.add(dataChunk);
    }

    for (int i = 0; i < blockletInfoColumnar.getMeasureLength().length; i++) {
      DataChunk dataChunk = new DataChunk();
      dataChunk.setChunk_meta(getChunkCompressionMeta());
      dataChunk.setRowMajor(false);
      //TODO : Once schema PR is merged and information needs to be passed here.
      dataChunk.setColumn_ids(new ArrayList<Integer>());
      dataChunk.setData_page_length(blockletInfoColumnar.getMeasureLength()[i]);
      dataChunk.setData_page_offset(blockletInfoColumnar.getMeasureOffset()[i]);
      //TODO : Right now the encodings are happening at runtime. change as per this encoders.
      List<Encoding> encodings = new ArrayList<Encoding>();
      encodings.add(Encoding.DELTA);
      dataChunk.setEncoders(encodings);
      //TODO writing dummy presence meta need to set actual presence
      //meta
      PresenceMeta presenceMeta = new PresenceMeta();
      presenceMeta.setPresent_bit_streamIsSet(true);
      presenceMeta
          .setPresent_bit_stream(blockletInfoColumnar.getMeasureNullValueIndex()[i].toByteArray());
      dataChunk.setPresence(presenceMeta);
      //TODO : PresenceMeta needs to be implemented and set here
      // dataChunk.setPresence(new PresenceMeta());
      //TODO : Need to write ValueCompression meta here.
      List<ByteBuffer> encoderMetaList = new ArrayList<ByteBuffer>();
      encoderMetaList.add(ByteBuffer.wrap(serializeEncoderMeta(
          createValueEncoderMeta(blockletInfoColumnar.getCompressionModel(), i))));
      dataChunk.setEncoder_meta(encoderMetaList);
      colDataChunks.add(dataChunk);
    }
    blockletInfo.setColumn_data_chunks(colDataChunks);

    return blockletInfo;
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
   * It converts FileFooter thrift object to list of BlockletInfoColumnar objects
   *
   * @param footer
   * @return
   */
  public static List<BlockletInfoColumnar> convertBlockletInfo(FileFooter footer)
      throws IOException {
    List<BlockletInfoColumnar> listOfNodeInfo =
        new ArrayList<BlockletInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (BlockletInfo blockletInfo : footer.getBlocklet_info_list()) {
      BlockletInfoColumnar blockletInfoColumnar = new BlockletInfoColumnar();
      blockletInfoColumnar.setNumberOfKeys(blockletInfo.getNum_rows());
      List<DataChunk> columnChunks = blockletInfo.getColumn_data_chunks();
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
      blockletInfoColumnar.setKeyLengths(keyLengths);
      blockletInfoColumnar.setKeyOffSets(keyOffSets);
      blockletInfoColumnar.setKeyBlockIndexOffSets(keyBlockIndexOffsets);
      blockletInfoColumnar.setKeyBlockIndexLength(keyBlockIndexLens);
      blockletInfoColumnar.setDataIndexMapOffsets(indexMapOffsets);
      blockletInfoColumnar.setDataIndexMapLength(indexMapLens);
      blockletInfoColumnar.setIsSortedKeyColumn(sortState);

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
      blockletInfoColumnar.setMeasureLength(msrLens);
      blockletInfoColumnar.setMeasureOffset(msrOffsets);
      blockletInfoColumnar.setCompressionModel(getValueCompressionModel(encoderMetas));
      listOfNodeInfo.add(blockletInfoColumnar);
    }

    setBlockletIndex(footer, listOfNodeInfo);
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
      LOGGER.error("Error while reading ValueEncoderMeta");
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

  private static void setBlockletIndex(FileFooter footer,
      List<BlockletInfoColumnar> listOfNodeInfo) {
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
