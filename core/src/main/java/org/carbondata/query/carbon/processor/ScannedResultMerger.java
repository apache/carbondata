package org.carbondata.query.carbon.processor;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.result.Result;

/**
 * Interface for merging the scanned result
 */
public interface ScannedResultMerger {

	/**
     * Below method will be used to add the scanned result
     *
     * @param scannedResult     scanned result
     * @throws QueryExecutionException
     * 			throw exception in case of failure 
     */
    void addScannedResult(Result scannedResult) throws QueryExecutionException;

    /**
     * Below method will be used to get the query result 
     * @return query result
     * @throws QueryExecutionException
     * 			throw exception in case of any failure 
     */
    CarbonIterator<Result> getQueryResultIterator() throws QueryExecutionException;
}
