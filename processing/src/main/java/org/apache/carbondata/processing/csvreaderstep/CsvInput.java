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

package org.apache.carbondata.processing.csvreaderstep;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.StandardLogService;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.graphgenerator.GraphGenerator;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.textfileinput.EncodingType;

/**
 * Read a simple CSV file
 * Just output Strings found in the file...
 */
public class CsvInput extends BaseStep implements StepInterface {
  private static final Class<?> PKG = CsvInput.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CsvInput.class.getName());
  /**
   * NUM_CORES_DEFAULT_VAL
   */
  private static final int NUM_CORES_DEFAULT_VAL = 2;
  /**
   * ReentrantLock getFileBlockLock
   */
  private final Object getBlockListLock = new Object();
  /**
   * ReentrantLock putRowLock
   */
  private final Object putRowLock = new Object();
  private CsvInputMeta meta;
  private CsvInputData data;
  /**
   * resultArray
   */
  private Future[] resultArray;
  private List<List<BlockDetails>> threadBlockList = new ArrayList<>();

  private ExecutorService exec;

  /**
   * If rddIteratorKey is not null, read data from RDD
   */
  private String rddIteratorKey = null;

  private CarbonIterator<CarbonIterator<String[]>> rddIterator;

  public CsvInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    LOGGER.info("** Using csv file **");
  }

  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    meta = (CsvInputMeta) smi;
    data = (CsvInputData) sdi;

    if (first) {
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordDictionaryValuesTotalTime(
          meta.getPartitionID(), System.currentTimeMillis());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordCsvInputStepTime(
          meta.getPartitionID(), System.currentTimeMillis());
      first = false;
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

      // We only run in parallel if we have at least one file to process
      // AND if we have more than one step copy running...
      //
      data.parallel = meta.isRunningInParallel() && data.totalNumberOfSteps > 1;

      // The conversion logic for when the lazy conversion is turned of is simple:
      // Pretend it's a lazy conversion object anyway and get the native type during
      // conversion.
      //
      data.convertRowMeta = data.outputRowMeta.clone();

      for (ValueMetaInterface valueMeta : data.convertRowMeta.getValueMetaList()) {
        valueMeta.setStorageType(ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
      }

      // Calculate the indexes for the filename and row number fields
      //
      data.filenameFieldIndex = -1;
      if (!Const.isEmpty(meta.getFilenameField()) && meta.isIncludingFilename()) {
        data.filenameFieldIndex = meta.getInputFields().length;
      }

      data.rownumFieldIndex = -1;
      if (!Const.isEmpty(meta.getRowNumField())) {
        data.rownumFieldIndex = meta.getInputFields().length;
        if (data.filenameFieldIndex >= 0) {
          data.rownumFieldIndex++;
        }
      }
      rddIteratorKey = StringUtils.isEmpty(meta.getRddIteratorKey()) ? null : meta
              .getRddIteratorKey();
    }

    // start multi-thread to process
    int numberOfNodes;
    try {
      numberOfNodes = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException exc) {
      numberOfNodes = NUM_CORES_DEFAULT_VAL;
    }
    if (rddIteratorKey == null) {
      BlockDetails[] blocksInfo = GraphGenerator.blockInfo.get(meta.getBlocksID());
      if (blocksInfo.length == 0) {
        //if isDirectLoad = true, and partition number > file num
        //then blocksInfo will get empty in some partition processing, so just return
        setOutputDone();
        return false;
      }

      if (numberOfNodes > blocksInfo.length) {
        numberOfNodes = blocksInfo.length;
      }

      //new the empty lists
      for (int pos = 0; pos < numberOfNodes; pos++) {
        threadBlockList.add(new ArrayList<BlockDetails>());
      }

      //block balance to every thread
      for (int pos = 0; pos < blocksInfo.length; ) {
        for (int threadNum = 0; threadNum < numberOfNodes; threadNum++) {
          if (pos < blocksInfo.length) {
            threadBlockList.get(threadNum).add(blocksInfo[pos++]);
          }
        }
      }
      LOGGER.info("*****************Started all csv reading***********");
      startProcess(numberOfNodes);
      LOGGER.info("*****************Completed all csv reading***********");
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordCsvInputStepTime(
              meta.getPartitionID(), System.currentTimeMillis());
    } else if (rddIteratorKey.startsWith(CarbonCommonConstants.RDDUTIL_UPDATE_KEY)) {
      scanRddIteratorForUpdate();
    }
    else {
      scanRddIterator(numberOfNodes);
    }
    setOutputDone();
    return false;
  }

  class RddScanCallable implements Callable<Void> {
    @Override public Void call() throws Exception {
      StandardLogService
          .setThreadName(("PROCESS_DataFrame_PARTITIONS"), Thread.currentThread().getName());
      try {
        String[] values = null;
        boolean hasNext = true;
        CarbonIterator<String[]> iter;
        boolean isInitialized = false;
        while (hasNext) {
          // Inovke getRddIterator to get a RDD[Row] iterator of a partition.
          // The RDD comes from the sub-query DataFrame in InsertInto statement.
          iter = getRddIterator(isInitialized);
          isInitialized = true;
          if (iter == null) {
            hasNext = false;
          } else {
            while (iter.hasNext()) {
              values = iter.next();
              synchronized (putRowLock) {
                putRow(data.outputRowMeta, values);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error(e, "Scan rdd during data load is terminated due to error.");
        throw e;
      }
      return null;
    }
  }

  private synchronized CarbonIterator<String[]> getRddIterator(boolean isInitialized) {
    if (!isInitialized) {
      rddIterator.initialize();
    }
    if (rddIterator.hasNext()) {
      return rddIterator.next();
    }
    return null;
  }

  private void scanRddIterator(int numberOfNodes) throws RuntimeException {
    rddIterator = RddInputUtils.getAndRemove(rddIteratorKey);
    if (rddIterator != null) {
      exec = Executors.newFixedThreadPool(numberOfNodes);
      List<Future<Void>> results = new ArrayList<Future<Void>>(numberOfNodes);
      RddScanCallable[] calls = new RddScanCallable[numberOfNodes];
      for (int i = 0; i < numberOfNodes; i++) {
        calls[i] = new RddScanCallable();
        results.add(exec.submit(calls[i]));
      }
      try {
        for (Future<Void> futrue : results) {
          futrue.get();
        }
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error(e, "Thread InterruptedException");
        throw new RuntimeException("Thread InterruptedException", e);
      } finally {
        exec.shutdownNow();
      }
    }
  }

  private void scanRddIteratorForUpdate() throws RuntimeException {
    Iterator<String[]> iterator = RddInpututilsForUpdate.getAndRemove(rddIteratorKey);
    if (iterator != null) {
      try {
        while (iterator.hasNext()) {
          putRow(data.outputRowMeta, iterator.next());
        }
      } catch (KettleException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        LOGGER.error(e, "Scan rdd during data load is terminated due to error.");
        throw e;
      }
    }
  }

  private void startProcess(final int numberOfNodes) throws RuntimeException {
    exec = Executors.newFixedThreadPool(numberOfNodes);

    Callable<Void> callable = new Callable<Void>() {
      @Override public Void call() throws RuntimeException {
        StandardLogService.setThreadName(("PROCESS_BLOCKS"), Thread.currentThread().getName());
        try {
          LOGGER.info("*****************started csv reading by thread***********");
          doProcessUnivocity();
          LOGGER.info("*****************Completed csv reading by thread***********");
        } catch (Throwable e) {
          LOGGER.error(e, "Thread is terminated due to error");
          throw new RuntimeException("Thread is terminated due to error : " + e.getMessage());
        }
        return null;
      }
    };
    List<Future<Void>> results = new ArrayList<Future<Void>>(10);
    for (int i = 0; i < numberOfNodes; i++) {
      results.add(exec.submit(callable));
    }

    resultArray = results.toArray(new Future[results.size()]);
    try {
      for (int j = 0; j < resultArray.length; j++) {
        resultArray[j].get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Thread InterruptedException", e);
    }
    finally {
      exec.shutdownNow();
    }
  }

  private void doProcessUnivocity() {
    List<BlockDetails> blocksListForProcess = null;
    synchronized (getBlockListLock) {
      //get the blocksList for this thread
      blocksListForProcess = threadBlockList.get(threadBlockList.size() - 1);
      threadBlockList.remove(threadBlockList.size() - 1);
    }
    long currentTimeMillis = System.currentTimeMillis();
    UnivocityCsvParser parser = new UnivocityCsvParser(getParserVo(blocksListForProcess));
    long numberOfRows = 0;
    int numberOfColumns = meta.getInputFields().length;
    try {
      parser.initialize();
      while (parser.hasMoreRecords()) {
        String[] next = parser.getNextRecord();
        if (next.length < numberOfColumns) {
          String[] temp = new String[numberOfColumns];
          System.arraycopy(next, 0, temp, 0, next.length);
          next = temp;
        }
        synchronized (putRowLock) {
          putRow(data.outputRowMeta, next);
          numberOfRows++;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (KettleException e) {
      throw new RuntimeException(e);
    }
    LOGGER.info("Total Number of records processed by this thread is: " + numberOfRows);
    LOGGER.info("Time taken to processed " + numberOfRows + " Number of records: " + (
        System.currentTimeMillis() - currentTimeMillis));
  }

  private UnivocityCsvParserVo getParserVo(List<BlockDetails> blocksListForProcess) {
    UnivocityCsvParserVo csvParserVo = new UnivocityCsvParserVo();
    csvParserVo.setBlockDetailsList(blocksListForProcess);
    csvParserVo.setDelimiter(meta.getDelimiter());
    csvParserVo.setNumberOfColumns(meta.getInputFields().length);
    csvParserVo.setEscapeCharacter(meta.getEscapeCharacter());
    csvParserVo.setHeaderPresent(meta.isHeaderPresent());
    csvParserVo.setQuoteCharacter(meta.getQuoteCharacter());
    csvParserVo.setCommentCharacter(meta.getCommentCharacter());
    String maxColumns = meta.getMaxColumns();
    if (null != maxColumns) {
      csvParserVo.setMaxColumns(Integer.parseInt(maxColumns));
    }
    return csvParserVo;
  }

  @Override public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    try {
      // Clean the block info in map
      if (GraphGenerator.blockInfo.get(meta.getBlocksID()) != null) {
        GraphGenerator.blockInfo.remove(meta.getBlocksID());
      }
    } catch (Exception e) {
      logError("Error closing file channel", e);
    }

    super.dispose(smi, sdi);
  }

  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CsvInputMeta) smi;
    data = (CsvInputData) sdi;

    if (super.init(smi, sdi)) {
      // If the step doesn't have any previous steps, we just get the filename.
      // Otherwise, we'll grab the list of filenames later...
      //
      if (getTransMeta().findNrPrevSteps(getStepMeta()) == 0) {
        String filename = environmentSubstitute(meta.getFilename());

        if (Const.isEmpty(filename) && Const.isEmpty(meta.getRddIteratorKey())) {
          logError(BaseMessages.getString(PKG, "CsvInput.MissingFilename.Message")); //$NON-NLS-1$
          return false;
        }
      }

      data.encodingType = EncodingType.guessEncodingType(meta.getEncoding());

      // PDI-2489 - set the delimiter byte value to the code point of the
      // character as represented in the input file's encoding
      try {
        data.delimiter = data.encodingType
            .getBytes(environmentSubstitute(meta.getDelimiter()), meta.getEncoding());
        data.escapeCharacter = data.encodingType
            .getBytes(environmentSubstitute(meta.getEscapeCharacter()), meta.getEncoding());
        if (Const.isEmpty(meta.getEnclosure())) {
          data.enclosure = null;
        } else {
          data.enclosure = data.encodingType
              .getBytes(environmentSubstitute(meta.getEnclosure()), meta.getEncoding());
        }

      } catch (UnsupportedEncodingException e) {
        logError(BaseMessages.getString(PKG, "CsvInput.BadEncoding.Message"), e); //$NON-NLS-1$
        return false;
      }

      // Handle parallel reading capabilities...
      //

      if (meta.isRunningInParallel()) {
        data.totalNumberOfSteps = getUniqueStepCountAcrossSlaves();

        // We are not handling a single file, but possibly a list of files...
        // As such, the fair thing to do is calculate the total size of the files
        // Then read the required block.
        //

      }
      return true;
    }
    return false;
  }

}
