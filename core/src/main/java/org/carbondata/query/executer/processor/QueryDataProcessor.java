package org.carbondata.query.executer.processor;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public interface QueryDataProcessor {

  public void processRow(ByteArrayWrapper key, MeasureAggregator[] value);

}
