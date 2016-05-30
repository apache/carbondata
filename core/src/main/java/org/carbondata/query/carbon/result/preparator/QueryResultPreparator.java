package org.carbondata.query.carbon.result.preparator;

import org.carbondata.query.carbon.result.Result;

public interface QueryResultPreparator<E> {

  public E prepareQueryResult(Result scannedResult);

}
