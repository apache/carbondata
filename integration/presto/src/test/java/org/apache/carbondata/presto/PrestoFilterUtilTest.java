/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.junit.Test;

import java.util.*;

import static com.facebook.presto.spi.predicate.Range.*;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.LONG;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PrestoFilterUtilTest {


    @Test
    public void testIntFilterExpression() {
        new MockUp<ColumnSchema>() {
            @Mock
            public DataType getDataType() {
                return LONG;
            }
        };
        Type spiType = CarbondataMetadata.carbonDataType2SpiMapper(new ColumnSchema());

        CarbondataColumnHandle cchmock = new CarbondataColumnHandle("", "columnName", spiType, 0, 2, 5, true, 1, "id", false, 0, 0);

        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, Domain.all(spiType));
                return Optional.of(mp);
            }
        };
        TupleDomain<ColumnHandle> td = TupleDomain.none();
        Expression expr = PrestoFilterUtil.parseFilterExpression(td);
        assertNull(expr); // check for empty range
        Long value = new Long(25);
        // Test for single value of bigint datatype
        Domain singleValue = Domain.singleValue(spiType, value);
        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, singleValue);
                return Optional.of(mp);
            }
        };
        Expression expr1 = PrestoFilterUtil.parseFilterExpression(td);
        assertTrue(expr1 instanceof EqualToExpression);
        //Test for multiple values of bigint datatype
        List valueList = new LinkedList<Long>();
        valueList.add(new Long(25));
        valueList.add(new Long(45));
        Domain multipleValue = Domain.multipleValues(spiType, valueList);
        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, multipleValue.complement());
                return Optional.of(mp);
            }
        };
        Expression expression = PrestoFilterUtil.parseFilterExpression(td);
        assertTrue (expression instanceof AndExpression);
    }

    @Test
    public void testStringFilterExpression() {

        Type spiType = VarcharType.VARCHAR;

        CarbondataColumnHandle cchmock = new CarbondataColumnHandle("", "columnName", spiType, 0, 2, 5, true, 1, "id", false, 0, 0);

        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, Domain.all(spiType));
                return Optional.of(mp);
            }
        };
        TupleDomain<ColumnHandle> td = TupleDomain.none();
        Domain singleValue = Domain.singleValue(spiType, Slices.utf8Slice("fakeString"));
        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, singleValue);
                return Optional.of(mp);
            }
        };
        Expression expr = PrestoFilterUtil.parseFilterExpression(td);
        assertTrue (expr instanceof EqualToExpression);

    }

    @Test
    public void testDateFilterExpression() {

        Type spiType = DateType.DATE;

        CarbondataColumnHandle cchmock = new CarbondataColumnHandle("", "columnName", spiType, 0, 2, 5, true, 1, "id", false, 0, 0);

        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, Domain.all(spiType));
                return Optional.of(mp);
            }
        };
        TupleDomain<ColumnHandle> td = TupleDomain.none();
        Domain singleValue = Domain.singleValue(spiType, new Long(55625));
        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, singleValue);
                return Optional.of(mp);
            }
        };
        Expression expr = PrestoFilterUtil.parseFilterExpression(td);
        assertTrue (expr instanceof EqualToExpression);

    }

    @Test
    public void testDecimalFilterExpression() {

        Type spiType = DecimalType.createDecimalType();

        CarbondataColumnHandle cchmock = new CarbondataColumnHandle("", "columnName", spiType, 0, 2, 5, true, 1, "id", false, 2, 0);

        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, Domain.all(spiType));
                return Optional.of(mp);
            }
        };
        TupleDomain<ColumnHandle> td = TupleDomain.none();
        Long unscaledDecimal =
                rescale(55252, 3, 5);
        Slice decimalSlice = Decimals.encodeUnscaledValue(unscaledDecimal);

        Domain singleValue = Domain.create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, 100L), greaterThanOrEqual(BIGINT, 200L)),false);
        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, singleValue);
                return Optional.of(mp);
            }
        };
        Expression expr = PrestoFilterUtil.parseFilterExpression(td);
        assertTrue (expr instanceof Expression);

    }

    @Test
    public void testGetFilters() {
        Expression expr = new ColumnExpression("", INT);
        PrestoFilterUtil.setFilter(2, expr);
        Expression getExpr = PrestoFilterUtil.getFilters(2);
        assert (getExpr instanceof ColumnExpression);

    }
}