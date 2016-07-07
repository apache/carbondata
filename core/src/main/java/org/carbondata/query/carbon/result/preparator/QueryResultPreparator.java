package org.carbondata.query.carbon.result.preparator;

import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.Result;

public interface QueryResultPreparator<K, V> {

  public BatchResult prepareQueryResult(Result<K, V> scannedResult);

}
