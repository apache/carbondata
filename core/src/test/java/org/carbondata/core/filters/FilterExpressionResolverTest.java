package org.carbondata.core.filters;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.leafnode.DataFileMetadata;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeBtreeIndex;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeIndex;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeMinMaxIndex;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.LiteralExpression;
import org.carbondata.query.expression.conditional.EqualToExpression;
import org.carbondata.query.filter.resolver.ConditionalFilterResolverImpl;
import org.carbondata.query.schema.metadata.FilterEvaluatorInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class FilterExpressionResolverTest {

  @Before public void setUp() throws Exception {

  }

  public void testConditionalResolver() {
    ColumnExpression columnExpression = new ColumnExpression("imei", DataType.StringType);
    LiteralExpression literalExpression = new LiteralExpression("1A001", DataType.StringType);
    EqualToExpression equalsToExpression =
        new EqualToExpression(columnExpression, literalExpression);
    ConditionalFilterResolverImpl condResolverImpl =
        new ConditionalFilterResolverImpl(equalsToExpression, false, true);
    FilterEvaluatorInfo info = new FilterEvaluatorInfo();
    //TableSegment tableSegment =new TableSegment();
    //tableSegment.loadSegmentBlock(getDataFileMetadataList(), null);
    //info.setTableSegment(tableSegment);
    //condResolverImpl.resolve(info);
    //info.setd
    //condResolverImpl.resolve(info);
  }

  private List<DataFileMetadata> getDataFileMetadataList() {
    List<DataFileMetadata> list = new ArrayList<DataFileMetadata>();
    try {
      int[] dimensionBitLength =
          CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
      KeyGenerator multiDimKeyVarLengthGenerator =
          new MultiDimKeyVarLengthGenerator(dimensionBitLength);
      int i = 1;
      while (i < 1001) {
        byte[] startKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i, i });
        byte[] endKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i + 10, i + 10 });
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1);
        buffer.rewind();
        buffer.put((byte) 1);
        buffer.putInt(i);
        buffer.array();
        byte[] noDictionaryStartKey = buffer.array();

        ByteBuffer buffer1 = ByteBuffer.allocate(4 + 1);
        buffer1.rewind();
        buffer1.put((byte) 1);
        buffer1.putInt(i + 10);
        buffer1.array();
        byte[] noDictionaryEndKey = buffer.array();
        DataFileMetadata fileMetadata =
            getFileMetadata(startKey, endKey, noDictionaryStartKey, noDictionaryEndKey);
        list.add(fileMetadata);
        i = i + 10;
      }
    } catch (Exception e) {
      return null;
    }
    return list;
  }

  private DataFileMetadata getFileMetadata(byte[] startKey, byte[] endKey,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) {
    DataFileMetadata dataFileMetadata = new DataFileMetadata();
    LeafNodeIndex index = new LeafNodeIndex();
    LeafNodeBtreeIndex btreeIndex = new LeafNodeBtreeIndex();
    ByteBuffer buffer = ByteBuffer.allocate(4 + startKey.length + 4 + noDictionaryStartKey.length);
    buffer.putInt(startKey.length);
    buffer.putInt(noDictionaryStartKey.length);
    buffer.put(startKey);
    buffer.put(noDictionaryStartKey);
    buffer.rewind();
    btreeIndex.setStartKey(buffer.array());
    ByteBuffer buffer1 = ByteBuffer.allocate(4 + startKey.length + 4 + noDictionaryEndKey.length);
    buffer1.putInt(endKey.length);
    buffer1.putInt(noDictionaryEndKey.length);
    buffer1.put(endKey);
    buffer1.put(noDictionaryEndKey);
    buffer1.rewind();
    btreeIndex.setEndKey(buffer1.array());
    LeafNodeMinMaxIndex minMax = new LeafNodeMinMaxIndex();
    minMax.setMaxValues(new byte[][] { endKey, noDictionaryEndKey });
    minMax.setMinValues(new byte[][] { startKey, noDictionaryStartKey });
    index.setBtreeIndex(btreeIndex);
    index.setMinMaxIndex(minMax);
    dataFileMetadata.setLeafNodeIndex(index);
    return dataFileMetadata;
  }

  @After public void tearDown() throws Exception {

  }

  @Test public void test() {
    fail("Not yet implemented");
  }

}
