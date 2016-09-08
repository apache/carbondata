package org.carbondata.processing.newflow.dictionaryencode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;

import org.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.carbondata.processing.newflow.DataField;
import org.carbondata.processing.newflow.DataLoadProcessorStep;
import org.carbondata.processing.newflow.encoding.FieldEncoder;
import org.carbondata.processing.newflow.encoding.impl.DictionaryFieldEncoderImpl;

/**
 * Encode data with dictionary values.
 */
public class DictionaryEncoderProcessorStepImpl implements DataLoadProcessorStep {

  private CarbonDataLoadConfiguration configuration;

  private DataLoadProcessorStep child;

  private FieldEncoder[] encoders;

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override
  public void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child) {
    this.configuration = configuration;
    this.child = child;
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache =
        cacheProvider.createCache(CacheType.FORWARD_DICTIONARY, configuration.getStoreLocation());
    List<FieldEncoder> fieldEncoders = new ArrayList<>();
    DataField[] output = child.getOutput();
    for (int i = 0; i < output.length; i++) {
      if (output[i].hasDictionaryEncoding()) {
        fieldEncoders.add(new DictionaryFieldEncoderImpl(output[i], cache,
            configuration.getTableIdentifier().getCarbonTableIdentifier(), i));
      }
    }
    encoders = fieldEncoders.toArray(new FieldEncoder[fieldEncoders.size()]);
    child.intialize(configuration, child);
  }

  @Override public Iterator<Object[]> execute() {
    final Iterator<Object[]> iterator = child.execute();
    return new CarbonIterator<Object[]>() {
      @Override public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override public Object[] next() {
        Object[] next = iterator.next();
        for (int i = 0; i < encoders.length; i++) {
          encoders[i].encode(next);
        }
        return next;
      }
    };
  }

  @Override public void close() {

  }
}
