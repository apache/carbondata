package org.apache.carbondata.core.scan.filter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.cache.dictionary.AbstractDictionaryCacheTest;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FilterExpressionProcessorTest extends AbstractDictionaryCacheTest {

  private ColumnSchema columnSchema;

  @Before public void setUp() throws Exception {
    init();
    this.databaseName = props.getProperty("database", "testSchema");
    this.tableName = props.getProperty("tableName", "carbon");
    this.carbonStorePath = props.getProperty("storePath", "carbonStore");
    carbonTableIdentifier =
        new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString());
    this.carbonStorePath = props.getProperty("storePath", "carbonStore");
    columnSchema = new ColumnSchema();
    columnSchema.setColumnName("IMEI");
    columnSchema.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema.setDataType(DataTypes.STRING);
    columnSchema.setDimensionColumn(true);
    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(Encoding.IMPLICIT);
    columnSchema.setEncodingList(encodingList);
  }

  @Test public void testGetFilterResolverBasedOnExpressionType()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    CarbonColumn carbonColumn = new CarbonColumn(columnSchema, 0, 0);
    ColumnExpression columnExpression = new ColumnExpression("IMEI", DataTypes.STRING);
    columnExpression.setCarbonColumn(carbonColumn);
    LiteralExpression literalExpression = new LiteralExpression("ImeiValue", DataTypes.STRING);
    InExpression equalToExpression = new InExpression(columnExpression, literalExpression);
    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
    Method method = FilterExpressionProcessor.class
        .getDeclaredMethod("getFilterResolverBasedOnExpressionType", ExpressionType.class,
            boolean.class, Expression.class, AbsoluteTableIdentifier.class, Expression.class);
    method.setAccessible(true);
    Object result = method
        .invoke(filterExpressionProcessor, ExpressionType.EQUALS, false, equalToExpression, null,
            null);
    Assert.assertTrue(result.getClass().getName()
        .equals("org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl"));
  }
}
