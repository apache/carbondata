package org.carbondata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.carbondata.query.carbon.executor.QueryExecutorFactory;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.result.iterator.ChunkRawRowIterartor;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads the data from Carbon store.
 */
public class CarbonRecordReader<T> extends RecordReader<Void, T> {

  private QueryModel queryModel;

  private CarbonReadSupport<T> readSupport;

  private CarbonIterator<Object[]> carbonIterator;

  public CarbonRecordReader(QueryModel queryModel, CarbonReadSupport<T> readSupport) {
    this.queryModel = queryModel;
    this.readSupport = readSupport;
  }

  @Override public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    CarbonInputSplit carbonInputSplit = (CarbonInputSplit) split;
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<TableBlockInfo>();
    tableBlockInfoList.add(
        new TableBlockInfo(carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
            carbonInputSplit.getSegmentId(), carbonInputSplit.getLocations(),
            carbonInputSplit.getLength()));
    queryModel.setTableBlockInfos(tableBlockInfoList);
    readSupport
        .intialize(queryModel.getProjectionColumns(), queryModel.getAbsoluteTableIdentifier());
    try {
      carbonIterator = new ChunkRawRowIterartor(
          (CarbonIterator<BatchRawResult>) QueryExecutorFactory.getQueryExecutor(queryModel)
              .execute(queryModel));
    } catch (QueryExecutionException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override public boolean nextKeyValue() {
    return carbonIterator.hasNext();

  }

  @Override public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override public T getCurrentValue() throws IOException, InterruptedException {
    return readSupport.readRow(carbonIterator.next());
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    // TODO : Implement it based on total number of rows it is going to retrive.
    return 0;
  }

  @Override public void close() throws IOException {
    // clear dictionary cache
    Map<String, Dictionary> columnToDictionaryMapping = queryModel.getColumnToDictionaryMapping();
    if (null != columnToDictionaryMapping) {
      for (Map.Entry<String, Dictionary> entry : columnToDictionaryMapping.entrySet()) {
        CarbonUtil.clearDictionaryCache(entry.getValue());
      }
    }
    // close read support
    readSupport.close();
  }
}
