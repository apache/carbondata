package org.carbondata.core.filters;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.LiteralExpression;
import org.carbondata.query.expression.conditional.EqualToExpression;
import org.carbondata.query.filter.resolver.ConditionalFilterResolverImpl;

import org.junit.After;
import org.junit.Before;

public class FilterExpressionResolverTest {

  @Before public void setUp() throws Exception {

  }

  public void testConditionalResolver() {
    ColumnExpression columnExpression = new ColumnExpression("imei", DataType.StringType);
    LiteralExpression literalExpression = new LiteralExpression("1A001", DataType.StringType);
    EqualToExpression equalsToExpression =
        new EqualToExpression(columnExpression, literalExpression);
    try {
      ConditionalFilterResolverImpl condResolverImpl =
          new ConditionalFilterResolverImpl(equalsToExpression, false, true);
      CarbonTableIdentifier carbonTableIdentifier =
          new CarbonTableIdentifier("database", "testSchema");
      AbsoluteTableIdentifier absoluteTableIdentifier =
          new AbsoluteTableIdentifier("storePath", carbonTableIdentifier);
      condResolverImpl.resolve(absoluteTableIdentifier);
      if (null != condResolverImpl.getDimColResolvedFilterInfo()) {
        assert (true);
      } else {
        assert (false);
      }
    } catch (Exception e) {
      assert (false);
    }

  }

  @After public void tearDown() throws Exception {

  }
}
