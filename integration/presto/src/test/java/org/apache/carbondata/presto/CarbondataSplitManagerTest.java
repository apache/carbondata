package org.apache.carbondata.presto;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableConfig;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import static com.facebook.presto.spi.predicate.Range.range;

public class CarbondataSplitManagerTest {

  private static TupleDomain<ColumnHandle> constraint;
  private static CarbondataColumnHandle columnHandle_1;
  private static CarbondataSplitManager splitManager;
  private static CarbondataConnectorId connectorId;
  private static CarbonTableReader carbonTableReader;
  private static CarbondataTableLayoutHandle carbondataTableLayoutHandle;
  private static ConnectorSession connectorSession;
  private static List<CarbonLocalInputSplit> inputSplits = new ArrayList<>();

  @Before public void setUp() {
    connectorSession = new ConnectorSession() {
      @Override public String getQueryId() {
        return null;
      }

      @Override public Identity getIdentity() {
        return null;
      }

      @Override public TimeZoneKey getTimeZoneKey() {
        return null;
      }

      @Override public Locale getLocale() {
        return null;
      }

      @Override public long getStartTime() {
        return 0;
      }

      @Override public <T> T getProperty(String name, Class<T> type) {
        return null;
      }
    };

    connectorId = new CarbondataConnectorId("connectorId1");
    carbonTableReader = new CarbonTableReader(new CarbonTableConfig());
    splitManager = new CarbondataSplitManager(connectorId, carbonTableReader);
  }

  @Test public void testGetColumnConstraints() {
    columnHandle_1 =
        new CarbondataColumnHandle("connectorId1", "id", IntegerType.INTEGER, 0, 0, 0, true, 0,
            "1234567890", true, 0, 0);

    constraint = TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle_1, Domain
        .create(ValueSet.ofRanges(range(IntegerType.INTEGER, 100L, false, 200L, true)), false)));

    List<CarbondataColumnConstraint> constraintList = splitManager.getColumnConstraints(constraint);
    Assert.assertTrue(constraintList.get(0).isInvertedindexed());
    Assert.assertEquals(constraintList.get(0).getName(), "id");
  }

  @Test public void testGetSplit() {
    new MockUp<CarbonTableReader>() {
      @Mock public CarbonTableCacheModel getCarbonCache(SchemaTableName table) {
        return new CarbonTableCacheModel();
      }

      @Mock
      public List<CarbonLocalInputSplit> getInputSplits2(CarbonTableCacheModel tableCacheModel,
          Expression filters) {
        inputSplits.add(
            new CarbonLocalInputSplit("segmentId", "path", 0, 5, new ArrayList<String>(), 5,
                Short.MAX_VALUE, new String[] { "d1", "d2" }, "detailInfo"));
        return inputSplits;
      }
    };

    CarbondataTableHandle carbondataTableHandle =
        new CarbondataTableHandle("connectorId", new SchemaTableName("schema1", "table1"));
    ColumnHandle columnHandle =
        new CarbondataColumnHandle("connectorid", "id", IntegerType.INTEGER, 0, 0, 0, true, 0,
            "1234567890", true, 0, 0);
    ImmutableMap<ColumnHandle, NullableValue> bindings =
        ImmutableMap.<ColumnHandle, NullableValue>builder()
            .put(columnHandle, NullableValue.of(IntegerType.INTEGER, 10L)).build();
    TupleDomain<ColumnHandle> columnHandleTupleDomain = TupleDomain.fromFixedValues(bindings);
    carbondataTableLayoutHandle =
        new CarbondataTableLayoutHandle(carbondataTableHandle, columnHandleTupleDomain);

    splitManager.getSplits(CarbondataTransactionHandle.INSTANCE, connectorSession,
        carbondataTableLayoutHandle);
  }

}
