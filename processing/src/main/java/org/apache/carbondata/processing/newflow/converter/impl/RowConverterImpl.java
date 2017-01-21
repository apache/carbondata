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
package org.apache.carbondata.processing.newflow.converter.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.converter.FieldConverter;
import org.apache.carbondata.processing.newflow.converter.RowConverter;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;

/**
 * It converts the complete row if necessary, dictionary columns are encoded with dictionary values
 * and nondictionary values are converted to binary.
 */
public class RowConverterImpl implements RowConverter {

  private CarbonDataLoadConfiguration configuration;

  private DataField[] fields;

  private FieldConverter[] fieldConverters;

  private BadRecordsLogger badRecordLogger;

  private BadRecordLogHolder logHolder;

  private DictionaryClient dictClient;

  private ExecutorService executorService;

  public RowConverterImpl(DataField[] fields, CarbonDataLoadConfiguration configuration,
      BadRecordsLogger badRecordLogger) {
    this.fields = fields;
    this.configuration = configuration;
    this.badRecordLogger = badRecordLogger;
  }

  @Override
  public void initialize() throws IOException {
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache =
        cacheProvider.createCache(CacheType.REVERSE_DICTIONARY,
            configuration.getTableIdentifier().getStorePath());
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    List<FieldConverter> fieldConverterList = new ArrayList<>();

    long lruCacheStartTime = System.currentTimeMillis();

    // for one pass load, start the dictionary client
    if (configuration.getUseOnePass()) {
      executorService = Executors.newFixedThreadPool(1);
      Future<DictionaryClient> result = executorService.submit(new Callable<DictionaryClient>() {
        @Override
        public DictionaryClient call() throws Exception {
          Thread.currentThread().setName("Dictionary client");
          DictionaryClient dictionaryClient = new DictionaryClient();
          dictionaryClient.startClient(configuration.getDictionaryServerHost(),
                  configuration.getDictionaryServerPort());
          return dictionaryClient;
        }
      });

      try {
        // wait for client initialization finished, or will raise null pointer exception
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      try {
        dictClient = result.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    for (int i = 0; i < fields.length; i++) {
      FieldConverter fieldConverter = FieldEncoderFactory.getInstance()
          .createFieldEncoder(fields[i], cache,
              configuration.getTableIdentifier().getCarbonTableIdentifier(), i, nullFormat,
              dictClient, configuration.getUseOnePass(),
              configuration.getTableIdentifier().getStorePath());
      fieldConverterList.add(fieldConverter);
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordLruCacheLoadTime((System.currentTimeMillis() - lruCacheStartTime) / 1000.0);
    fieldConverters = fieldConverterList.toArray(new FieldConverter[fieldConverterList.size()]);
    logHolder = new BadRecordLogHolder();
  }

  @Override
  public CarbonRow convert(CarbonRow row) throws CarbonDataLoadingException {
    CarbonRow copy = row.getCopy();
    for (int i = 0; i < fieldConverters.length; i++) {
      fieldConverters[i].convert(row, logHolder);
      if (logHolder.isBadRecordNotAdded()) {
        badRecordLogger.addBadRecordsToBuilder(copy.getData(), logHolder.getReason());
        logHolder.clear();
        if(badRecordLogger.isBadRecordConvertNullDisable()) {
          return null;
        }
      }
    }
    return row;
  }

  @Override
  public void finish() {
    List<Integer> dimCardinality = new ArrayList<>();
    for (int i = 0; i < fieldConverters.length; i++) {
      if (fieldConverters[i] instanceof AbstractDictionaryFieldConverterImpl) {
        ((AbstractDictionaryFieldConverterImpl) fieldConverters[i])
            .fillColumnCardinality(dimCardinality);
      }
    }
    int[] cardinality = new int[dimCardinality.size()];
    for (int i = 0; i < dimCardinality.size(); i++) {
      cardinality[i] = dimCardinality.get(i);
    }
    // Set the cardinality to configuration, it will be used by further step for mdk key.
    configuration.setDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS, cardinality);

    // close dictionary client when finish write
    if (configuration.getUseOnePass()) {
      dictClient.shutDown();
      executorService.shutdownNow();
    }
  }

  @Override
  public RowConverter createCopyForNewThread() {
    RowConverterImpl converter =
        new RowConverterImpl(this.fields, this.configuration, this.badRecordLogger);
    converter.fieldConverters = fieldConverters;
    converter.logHolder = new BadRecordLogHolder();
    return converter;
  }

}
