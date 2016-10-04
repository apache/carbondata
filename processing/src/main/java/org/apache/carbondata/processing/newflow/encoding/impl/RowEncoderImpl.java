package org.apache.carbondata.processing.newflow.encoding.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.encoding.RowEncoder;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;

public class RowEncoderImpl implements RowEncoder {

  private AbstractDictionaryFieldEncoderImpl[] dictionaryFieldEncoders;

  private NonDictionaryFieldEncoderImpl[] nonDictionaryFieldEncoders;

  private MeasureFieldEncoderImpl[] measureFieldEncoders;

  private KeyGenerator keyGenerator;

  public RowEncoderImpl(DataField[] fields, CarbonDataLoadConfiguration configuration) {
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache = cacheProvider
        .createCache(CacheType.REVERSE_DICTIONARY,
            configuration.getTableIdentifier().getStorePath());
    List<AbstractDictionaryFieldEncoderImpl> dictFieldEncoders = new ArrayList<>();
    List<NonDictionaryFieldEncoderImpl> nonDictFieldEncoders = new ArrayList<>();
    List<MeasureFieldEncoderImpl> measureFieldEncoderList = new ArrayList<>();

    long lruCacheStartTime = System.currentTimeMillis();

    for (int i = 0; i < fields.length; i++) {
      FieldEncoder fieldEncoder = FieldEncoderFactory.getInstance()
          .createFieldEncoder(fields[i], cache,
              configuration.getTableIdentifier().getCarbonTableIdentifier(), i);
      if (fieldEncoder instanceof AbstractDictionaryFieldEncoderImpl) {
        dictFieldEncoders.add((AbstractDictionaryFieldEncoderImpl) fieldEncoder);
      } else if (fieldEncoder instanceof NonDictionaryFieldEncoderImpl) {
        nonDictFieldEncoders.add((NonDictionaryFieldEncoderImpl) fieldEncoder);
      } else if (fieldEncoder instanceof MeasureFieldEncoderImpl) {
        measureFieldEncoderList.add((MeasureFieldEncoderImpl)fieldEncoder);
      }
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordLruCacheLoadTime((System.currentTimeMillis() - lruCacheStartTime) / 1000.0);
    dictionaryFieldEncoders =
        dictFieldEncoders.toArray(new AbstractDictionaryFieldEncoderImpl[dictFieldEncoders.size()]);
    nonDictionaryFieldEncoders = nonDictFieldEncoders
        .toArray(new NonDictionaryFieldEncoderImpl[nonDictFieldEncoders.size()]);
    measureFieldEncoders = measureFieldEncoderList
        .toArray(new MeasureFieldEncoderImpl[measureFieldEncoderList.size()]);

    int[] dimCardinality = new int[dictionaryFieldEncoders.length];
    for (int i = 0; i < dimCardinality.length; i++) {
      dimCardinality[i] = dictionaryFieldEncoders[i].getColumnCardinality();
    }

    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        configuration.getTableIdentifier().getCarbonTableIdentifier().getTableUniqueName());
    String tableName = configuration.getTableIdentifier().getCarbonTableIdentifier().getTableName();
    List<ColumnSchema> columnSchemaList = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(tableName),
            carbonTable.getMeasureByTableName(tableName));
    int[] formattedCardinality =
        CarbonUtil.getFormattedCardinality(dimCardinality, columnSchemaList);
    SegmentProperties segmentProperties =
        new SegmentProperties(columnSchemaList, formattedCardinality);
    keyGenerator = segmentProperties.getDimensionKeyGenerator();
  }

  @Override public Object[] encode(Object[] row) {

    int[] dictArray = new int[dictionaryFieldEncoders.length];
    for (int i = 0; i < dictArray.length; i++) {
      dictArray[i] = dictionaryFieldEncoders[i].encode(row);
    }
    byte[] generateKey = keyGenerator.generateKey(dictArray);

    ByteBuffer[] byteBuffers = new ByteBuffer[nonDictionaryFieldEncoders.length];
    for (int i = 0; i < byteBuffers.length; i++) {
      byteBuffers[i] = nonDictionaryFieldEncoders[i].encode(row);
    }

    byte[] nonDictionaryCols =
        RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBuffers);

    Object[] measureArray = new Object[measureFieldEncoders.length];
    for (int i = 0; i < measureFieldEncoders.length; i++) {
      measureArray[i] = measureFieldEncoders[i].encode(row);
    }

    Object[] newOutArr = new Object[3];
    newOutArr[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()] = generateKey;
    newOutArr[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()] = nonDictionaryCols;
    newOutArr[IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex()] = measureArray;


    return newOutArr;
  }

}
