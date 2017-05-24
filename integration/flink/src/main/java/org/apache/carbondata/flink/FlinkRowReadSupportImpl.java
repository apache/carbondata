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

package org.apache.carbondata.flink;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;


public class FlinkRowReadSupportImpl<T> extends DictionaryDecodeReadSupport<T> {

    private static final long SECONDS_PER_DAY = 60 * 60 * 24L;
    private static final long MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L;
    protected Dictionary[] dictionaries;
    protected DataType[] dataTypes;
    /**
     * carbon columns
     */
    protected CarbonColumn[] carbonColumns;

    @Override
    public void initialize(CarbonColumn[] carbonColumns,
                           AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException {
        this.carbonColumns = carbonColumns;
        dictionaries = new Dictionary[carbonColumns.length];
        dataTypes = new DataType[carbonColumns.length];
        for (int i = 0; i < carbonColumns.length; i++) {
            if (carbonColumns[i].hasEncoding(Encoding.DICTIONARY) && !carbonColumns[i]
                    .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                CacheProvider cacheProvider = CacheProvider.getInstance();
                Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
                        .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath());
                dataTypes[i] = carbonColumns[i].getDataType();
                dictionaries[i] = forwardDictionaryCache.get(new DictionaryColumnUniqueIdentifier(
                        absoluteTableIdentifier.getCarbonTableIdentifier(),
                        carbonColumns[i].getColumnIdentifier(), dataTypes[i]));
            } else {
                dataTypes[i] = carbonColumns[i].getDataType();
            }
        }
    }

    @Override
    public T readRow(Object[] data) {
        for (int i = 0; i < dictionaries.length; i++) {
            if (dictionaries[i] != null) {
                int key = Integer.parseInt(String.valueOf(data[i]));
                data[i] = dictionaries[i].getDictionaryValueForKey(key);
            } else if (carbonColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                //convert the long to timestamp in case of direct dictionary column
                if (DataType.TIMESTAMP == carbonColumns[i].getDataType()) {
                    if (data[i] == null) {
                        data[i] = data[i];
                    } else {
                        String timestampFormat = CarbonProperties.getInstance().getProperty(
                                CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
                        long timestamp = Long.parseLong(data[i].toString());
                        Timestamp transformedTimestamp = new Timestamp((timestamp) / 1000 );
                        SimpleDateFormat simpleTimestampFormat = new SimpleDateFormat(timestampFormat);
                        data[i] = simpleTimestampFormat.format(transformedTimestamp);
                    }
                    //convert the long to date in case of direct dictionary column
                } else if (DataType.DATE == carbonColumns[i].getDataType()) {
                    if (data[i] == null) {
                        data[i] = data[i];
                    } else {
                        String dateFormat = CarbonProperties.getInstance().getProperty(
                                CarbonCommonConstants.CARBON_DATE_FORMAT,
                                CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
                        Long date = Long.parseLong(String.valueOf(data[i])) * MILLIS_PER_DAY;
                        Date millisTransformedDate = new Date(date);
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
                        data[i] = simpleDateFormat.format(millisTransformedDate);
                    }
                }
            } else {
                data[i] = data[i];
            }
        }
        return (T) data;
    }

    @Override
    public void close() {
        for (int i = 0; i < dictionaries.length; i++) {
            CarbonUtil.clearDictionaryCache(dictionaries[i]);
        }
    }

}
