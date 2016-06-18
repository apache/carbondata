package org.carbondata.scan.result.preparator;

import org.carbondata.scan.result.BatchResult;
import org.carbondata.scan.result.Result;

public interface QueryResultPreparator<K, V> {

  public BatchResult prepareQueryResult(Result<K, V> scannedResult);

}
