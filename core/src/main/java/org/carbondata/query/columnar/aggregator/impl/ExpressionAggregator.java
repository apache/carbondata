/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.columnar.aggregator.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.query.aggregator.CustomMeasureAggregator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.columnar.aggregator.ColumnarAggregatorInfo;
import org.carbondata.query.columnar.aggregator.impl.dimension.DimensionDataAggreagtor;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.carbonfilterinterface.RowImpl;
import org.carbondata.query.util.DataTypeConverter;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.QueryExecutorUtility;

/**
 * To handle aggregation for expressions in the query
 *
 * @author K00900841
 */
public class ExpressionAggregator {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DimensionDataAggreagtor.class.getName());

    private ColumnarAggregatorInfo columnaraggreagtorInfo;

    public ExpressionAggregator(ColumnarAggregatorInfo columnaraggreagtorInfo) {
        this.columnaraggreagtorInfo = columnaraggreagtorInfo;
    }

    public void aggregateExpression(AbstractColumnarScanResult keyValue,
            MeasureAggregator[] currentMsrRowData) {
        int startIndex = this.columnaraggreagtorInfo.getExpressionStartIndex();
        RowImpl rowImpl = null;
        for (int i = 0; i < columnaraggreagtorInfo.getCustomExpressions().size(); i++) {
            List<Dimension> referredColumns =
                    columnaraggreagtorInfo.getCustomExpressions().get(i).getReferredColumns();
            Object[] row = new Object[referredColumns.size()];
            for (int j = 0; j < referredColumns.size(); j++) {
                Dimension dimension = referredColumns.get(j);
                if (dimension instanceof Measure) {
                    switch (dimension.getDataType()) {
                    case LONG:
                        row[j] = keyValue.getLongValue(dimension.getOrdinal());
                        break;
                    case DECIMAL:
                        row[j] = keyValue.getBigDecimalValue(dimension.getOrdinal());
                        break;
                    default:
                        row[j] = keyValue.getDoubleValue(dimension.getOrdinal());
                    }
                } else if (dimension.isNoDictionaryDim()) {
                    byte[] noDictionaryVal =
                            keyValue.getNo_DictionayDimDataForAgg(dimension.getOrdinal());
                    row[j] = DataTypeConverter.getDataBasedOnDataType(new String(noDictionaryVal),
                            dimension.getDataType());
                } else {
                    if (dimension.getDataType() != Type.ARRAY
                            && dimension.getDataType() != Type.STRUCT) {
                        int dimSurrogate = keyValue.getDimDataForAgg(dimension.getOrdinal());
                        if (dimSurrogate == 1) {
                            row[j] = null;
                        } else {
                            String member = QueryExecutorUtility
                                    .getMemberBySurrogateKey(dimension, dimSurrogate,
                                            columnaraggreagtorInfo.getSlices(),
                                            columnaraggreagtorInfo.getCurrentSliceIndex())
                                    .toString();
                            row[j] = DataTypeConverter
                                    .getDataBasedOnDataType(member, dimension.getDataType());
                        }
                    } else {
                        try {
                            GenericQueryType complexType = null;
                            complexType = this.columnaraggreagtorInfo.getComplexQueryDims()
                                    .get(dimension.getOrdinal());
                            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                            DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
                            keyValue.getComplexDimDataForAgg(complexType, dataOutputStream);
                            row[j] = complexType.getDataBasedOnDataTypeFromSurrogates(
                                    this.columnaraggreagtorInfo.getSlices(),
                                    ByteBuffer.wrap(byteStream.toByteArray()),
                                    this.columnaraggreagtorInfo.getDimensions());
                            byteStream.close();
                        } catch (IOException e) {
                            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
                        }
                    }
                }
            }
            CustomMeasureAggregator agg =
                    (CustomMeasureAggregator) currentMsrRowData[startIndex + i];
            rowImpl = new RowImpl();
            rowImpl.setValues(row);
            agg.agg(rowImpl);
        }

    }
}
