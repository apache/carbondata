package org.apache.carbondata.core.indexstore.blockletindex;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.cache.dictionary.AbstractDictionaryCacheTest;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.statistics.BlockletStatistics;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonImplicitDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.ByteUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

public class TestBlockletDataMap extends AbstractDictionaryCacheTest {

  ImplicitIncludeFilterExecutorImpl implicitIncludeFilterExecutor;
  @Before public void setUp() throws Exception {
    CarbonImplicitDimension carbonImplicitDimension =
        new CarbonImplicitDimension(0, CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_POSITIONID);
    DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = new DimColumnResolvedFilterInfo();
    dimColumnEvaluatorInfo.setColumnIndex(0);
    dimColumnEvaluatorInfo.setRowIndex(0);
    dimColumnEvaluatorInfo.setDimension(carbonImplicitDimension);
    dimColumnEvaluatorInfo.setDimensionExistsInCurrentSilce(false);
    implicitIncludeFilterExecutor =
        new ImplicitIncludeFilterExecutorImpl(dimColumnEvaluatorInfo);
  }

  @Test public void testaddBlockBasedOnMinMaxValue() throws Exception {

    new MockUp<ImplicitIncludeFilterExecutorImpl>() {
      @Mock BitSet isFilterValuesPresentInBlockOrBlocklet(BlockletStatistics blockletStatistics,
          String uniqueBlockPath) {
        BitSet bitSet = new BitSet(1);
        bitSet.set(8);
        return bitSet;
      }
    };

    BlockletDataMap blockletDataMap = new BlockletDataMap();
    Method method = BlockletDataMap.class
        .getDeclaredMethod("addBlockBasedOnMinMaxValue", FilterExecuter.class,
            BlockletStatistics.class, String.class, int.class);
    method.setAccessible(true);

    byte[][] minValue = { ByteUtil.toBytes("sfds") };
    byte[][] maxValue = { ByteUtil.toBytes("resa") };
    BitSet nullValue = new BitSet(0);
    BlockletStatistics blockletStatistics = new BlockletStatistics(maxValue, minValue, nullValue);

    Object result = method
        .invoke(blockletDataMap, implicitIncludeFilterExecutor, blockletStatistics,
            "/opt/store/default/carbon_table/Fact/Part0/Segment_0/part-0-0_batchno0-0-1514989110586.carbondata",
            0);
    assert ((boolean) result);
  }
}
