package org.apache.carbondata.scan.filter.executer;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.block.BlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockletBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.ChunkCompressorMeta;
import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.CompressionCodec;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.sort.SortState;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;

import mockit.Mock;
import mockit.MockUp;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Created by harmeet on 24/11/16.
 */
public class FilterExecutorTestRule implements TestRule {

  SegmentProperties segmentProperties;
  ColumnSchema columnSchema1, columnSchema2, columnSchema3, columnSchema4;
  CarbonDimension carbonDimension;
  DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo;
  MeasureColumnResolvedFilterInfo filterInfo;
  Expression greaterThanEqualsTo;
  AbsoluteTableIdentifier tableIdentifier;
  DimColumnFilterInfo dimColumnFilterInfo;
  BTreeBuilderInfo bTreeBuilderInfo;
  BlockletBTreeLeafNode blockletBTreeLeafNode;

  @Override public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override public void evaluate() throws Throwable {
        List<Encoding> encodeList = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);

        columnSchema1 =
            getWrapperDimensionColumn(DataType.INT, "ID", true, Collections.<Encoding>emptyList(),
                false, -1);
        columnSchema2 = getWrapperDimensionColumn(DataType.INT, "salary", true,
            Collections.<Encoding>emptyList(), false, -1);
        columnSchema3 =
            getWrapperDimensionColumn(DataType.STRING, "country", false, encodeList, true, 0);
        columnSchema4 =
            getWrapperDimensionColumn(DataType.STRING, "serialname", false, encodeList, true, 0);

        List<ColumnSchema> wrapperColumnSchema =
            Arrays.asList(columnSchema1, columnSchema2, columnSchema3, columnSchema4);

        segmentProperties = new SegmentProperties(wrapperColumnSchema, new int[] { 3, 11 });

        dimColumnFilterInfo = new DimColumnFilterInfo();
        dimColumnFilterInfo.setIncludeFilter(false);
        dimColumnFilterInfo.setFilterList(Arrays.asList(1, 4, 7));

        carbonDimension = new CarbonDimension(columnSchema4, 1, 1, 1, -1);

        dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
        dimColumnResolvedFilterInfo
            .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
        dimColumnResolvedFilterInfo.setDimension(carbonDimension);
        dimColumnResolvedFilterInfo.setColumnIndex(0);
        dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

        filterInfo = new MeasureColumnResolvedFilterInfo();
        filterInfo.setColumnIndex(1);
        filterInfo.setRowIndex(0);
        filterInfo.setMeasureExistsInCurrentSlice(true);
        filterInfo.setType(DataType.INT);

        ColumnExpression right = new ColumnExpression("contact", DataType.DECIMAL);
        right.setColIndex(0);
        ColumnExpression left = new ColumnExpression("contact", DataType.DECIMAL);
        left.setColIndex(1);
        greaterThanEqualsTo = new GreaterThanEqualToExpression(left, right);

        CarbonTableIdentifier carbonTableIdentifier =
            new CarbonTableIdentifier("default", "t3", "1479811576514");
        tableIdentifier = new AbsoluteTableIdentifier(new File("target/store").getAbsolutePath(),
            carbonTableIdentifier);

        List<DataFileFooter> footerList = getDataFileFooterList();
        bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

        blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

        statement.evaluate();
      }
    };
  }

  private ColumnSchema getWrapperDimensionColumn(DataType dataType, String columnName,
      boolean columnar, List<Encoding> encodeList, boolean dimensionColumn, int columnGroup) {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setDataType(dataType);
    dimColumn.setColumnName(columnName);
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setColumnar(columnar);

    dimColumn.setEncodingList(encodeList);
    dimColumn.setDimensionColumn(dimensionColumn);
    dimColumn.setUseInvertedIndex(true);
    dimColumn.setColumnGroup(columnGroup);
    return dimColumn;
  }

  private DataChunk getDataChunk(int dataPageOffset, int dataLengthPage, int rowIdPageOffset,
      int rowIdPageLength, int rlePageOffset, int rlePageLength, SortState sortState,
      List<Encoding> encodingList) {
    DataChunk dataChunk = new DataChunk();
    ChunkCompressorMeta chunkCompressorMeta = new ChunkCompressorMeta();
    chunkCompressorMeta.setCompressor(CompressionCodec.SNAPPY);

    dataChunk.setChunkCompressionMeta(chunkCompressorMeta);
    dataChunk.setRowMajor(false);
    dataChunk.setColumnUniqueIdList(Collections.EMPTY_LIST);
    dataChunk.setDataPageOffset(dataPageOffset);
    dataChunk.setDataPageLength(dataLengthPage);
    dataChunk.setRowIdPageOffset(rowIdPageOffset);
    dataChunk.setRowIdPageLength(rowIdPageLength);
    dataChunk.setRlePageOffset(rlePageOffset);
    dataChunk.setRlePageLength(rlePageLength);
    dataChunk.setRleApplied(false);
    dataChunk.setSortState(sortState);
    dataChunk.setEncoderList(encodingList);
    dataChunk.setNoDictonaryColumn(false);

    BitSet bitSet = new BitSet(1);
    bitSet.flip(1);

    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setBitSet(bitSet);
    presenceMeta.setRepresentNullValues(false);

    dataChunk.setNullValueIndexForColumn(presenceMeta);

    return dataChunk;
  }

  List<DataFileFooter> getDataFileFooterList() {
    DataFileFooter fileFooter = new DataFileFooter();

    fileFooter.setVersionId(1);
    fileFooter.setNumberOfRows(10);

    SegmentInfo segmentInfo = new SegmentInfo();
    segmentInfo.setNumberOfColumns(4);
    segmentInfo.setColumnCardinality(new int[] { 3, 11 });
    fileFooter.setSegmentInfo(segmentInfo);

    BlockletIndex blockletIndex = new BlockletIndex();
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex();
    blockletBTreeIndex.setStartKey(new byte[] { 0, 0, 0, 2, 0, 0, 0, 0, 2, 2 });
    blockletBTreeIndex.setEndKey(new byte[] { 0, 0, 0, 2, 0, 0, 0, 0, 3, 3 });
    blockletIndex.setBtreeIndex(blockletBTreeIndex);

    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    blockletMinMaxIndex.setMaxValues(new byte[][] { { 3 }, { 11 } });
    blockletMinMaxIndex.setMinValues(new byte[][] { { 2 }, { 2 } });
    blockletIndex.setMinMaxIndex(blockletMinMaxIndex);

    fileFooter.setBlockletIndex(blockletIndex);
    fileFooter.setColumnInTable(
        Arrays.asList(columnSchema1, columnSchema2, columnSchema3, columnSchema4));

    DataChunk dimenssionDataChunk1, dimenssionDataChunk2, measureDataChunk1, measureDataChunk2;
    dimenssionDataChunk1 = getDataChunk(0, 4, 0, 0, 136, 9, SortState.SORT_EXPLICT,
        Arrays.asList(Encoding.DICTIONARY, Encoding.RLE));
    dimenssionDataChunk2 = getDataChunk(4, 12, 120, 16, 145, 0, SortState.SORT_NATIVE,
        Arrays.asList(Encoding.DICTIONARY, Encoding.RLE, Encoding.INVERTED_INDEX));
    measureDataChunk1 =
        getDataChunk(16, 52, 0, 0, 0, 0, SortState.SORT_NONE, Arrays.asList(Encoding.DELTA));

    ValueEncoderMeta measureMeta1 = new ValueEncoderMeta();
    measureMeta1.setMaxValue(10l);
    measureMeta1.setMinValue(1l);
    measureMeta1.setUniqueValue(0l);
    measureMeta1.setDecimal(1);
    measureMeta1.setDataTypeSelected((byte) 0);
    measureMeta1.setType('l');
    measureDataChunk1.setValueEncoderMeta(Arrays.asList(measureMeta1));

    measureDataChunk2 =
        getDataChunk(68, 52, 0, 0, 0, 0, SortState.SORT_NONE, Arrays.asList(Encoding.DELTA));

    ValueEncoderMeta measureMeta2 = new ValueEncoderMeta();
    measureMeta2.setMaxValue(15009l);
    measureMeta2.setMinValue(15000l);
    measureMeta2.setUniqueValue(14999l);
    measureMeta2.setDecimal(1);
    measureMeta2.setDataTypeSelected((byte) 0);
    measureMeta2.setType('l');
    measureDataChunk2.setValueEncoderMeta(Arrays.asList(measureMeta2));

    BlockletInfo blockletInfo = new BlockletInfo();
    blockletInfo.setDimensionColumnChunk(Arrays.asList(dimenssionDataChunk1, dimenssionDataChunk2));
    blockletInfo.setMeasureColumnChunk(Arrays.asList(measureDataChunk1, measureDataChunk2));
    blockletInfo.setNumberOfRows(10);
    blockletInfo.setBlockletIndex(blockletIndex);

    new MockUp<DataFileFooter>() {

      @Mock public BlockInfo getBlockInfo() {

        final String filePath =
            this.getClass().getClassLoader().getResource("sample.carbondata").getPath();
        return new BlockInfo(
            new TableBlockInfo(filePath, 0, "0", new String[] { "localhost" }, 1324));
      }
    };

    fileFooter.setBlockletList(Arrays.asList(blockletInfo));

    return Arrays.asList(fileFooter);
  }
}
