/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.csvreaderstep;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.StandardLogService;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.load.BlockDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.graphgenerator.GraphGenerator;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
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
  private boolean isTerminated;
  private List<List<BlockDetails>> threadBlockList = new ArrayList<>();

  private ExecutorService exec;

  public CsvInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    LOGGER.info("** Using csv file **");
  }

  /**
   * This method is borrowed from TextFileInput
   *
   * @param log
   * @param line
   * @param delimiter
   * @param enclosure
   * @param escapeCharacter
   * @return
   * @throws KettleException
   */
  public static final String[] guessStringsFromLine(LogChannelInterface log, String line,
      String delimiter, String enclosure, String escapeCharacter) throws KettleException {
    List<String> strings = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    String pol; // piece of line

    try {
      if (line == null) {
        return null;
      }

      // Split string in pieces, only for CSV!
      int pos = 0;
      int length = line.length();
      boolean dencl = false;

      int lenEncl = (enclosure == null ? 0 : enclosure.length());
      int lenEsc = (escapeCharacter == null ? 0 : escapeCharacter.length());

      while (pos < length) {
        int from = pos;
        int next;

        boolean enclFound;
        boolean containsEscapedEnclosures = false;
        boolean containsEscapedSeparators = false;

        // Is the field beginning with an enclosure?
        // "aa;aa";123;"aaa-aaa";000;...
        if (lenEncl > 0 && line.substring(from, from + lenEncl).equalsIgnoreCase(enclosure)) {
          if (log.isRowLevel()) {
            log.logRowlevel(BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRow",
                    line.substring(from, from + lenEncl))); //$NON-NLS-1$ //$NON-NLS-2$
          }
          enclFound = true;
          int p = from + lenEncl;

          boolean isEnclosure =
              lenEncl > 0 && p + lenEncl < length && line.substring(p, p + lenEncl)
                  .equalsIgnoreCase(enclosure);
          boolean isEscape = lenEsc > 0 && p + lenEsc < length && line.substring(p, p + lenEsc)
              .equalsIgnoreCase(escapeCharacter);

          boolean enclosureAfter = false;

          // Is it really an enclosure? See if it's not repeated twice or escaped!
          if ((isEnclosure || isEscape) && p < length - 1) {
            String strnext = line.substring(p + lenEncl, p + 2 * lenEncl);
            if (strnext.equalsIgnoreCase(enclosure)) {
              p++;
              enclosureAfter = true;
              dencl = true;

              // Remember to replace them later on!
              if (isEscape) {
                containsEscapedEnclosures = true;
              }
            }
          }

          // Look for a closing enclosure!
          while ((!isEnclosure || enclosureAfter) && p < line.length()) {
            p++;
            enclosureAfter = false;
            isEnclosure = lenEncl > 0 && p + lenEncl < length && line.substring(p, p + lenEncl)
                .equals(enclosure);
            isEscape = lenEsc > 0 && p + lenEsc < length && line.substring(p, p + lenEsc)
                .equals(escapeCharacter);

            // Is it really an enclosure? See if it's not repeated twice or escaped!
            if ((isEnclosure || isEscape) && p < length - 1) // Is
            {
              String strnext = line.substring(p + lenEncl, p + 2 * lenEncl);
              if (strnext.equals(enclosure)) {
                p++;
                enclosureAfter = true;
                dencl = true;

                // Remember to replace them later on!
                if (isEscape) {
                  containsEscapedEnclosures = true; // remember
                }
              }
            }
          }

          if (p >= length) {
            next = p;
          } else {
            next = p + lenEncl;
          }

          if (log.isRowLevel()) {
            log.logRowlevel(BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                BaseMessages.getString(PKG, "CsvInput.Log.EndOfEnclosure",
                    "" + p)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          }
        } else {
          enclFound = false;
          boolean found = false;
          int startpoint = from;
          do {
            next = line.indexOf(delimiter, startpoint);

            // See if this position is preceded by an escape character.
            if (lenEsc > 0 && next - lenEsc > 0) {
              String before = line.substring(next - lenEsc, next);

              if (escapeCharacter != null && escapeCharacter.equals(before)) {
                // take the next separator, this one is escaped...
                startpoint = next + 1;
                containsEscapedSeparators = true;
              } else {
                found = true;
              }
            } else {
              found = true;
            }
          } while (!found && next >= 0);
        }
        if (next == -1) {
          next = length;
        }

        if (enclFound) {
          pol = line.substring(from + lenEncl, next - lenEncl);
          if (log.isRowLevel()) {
            log.logRowlevel(BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                BaseMessages.getString(PKG, "CsvInput.Log.EnclosureFieldFound",
                    "" + pol)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          }
        } else {
          pol = line.substring(from, next);
          if (log.isRowLevel()) {
            log.logRowlevel(BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                BaseMessages.getString(PKG, "CsvInput.Log.NormalFieldFound",
                    "" + pol)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          }
        }

        if (dencl) {
          StringBuilder sbpol = new StringBuilder(pol);
          int idx = sbpol.indexOf(enclosure + enclosure);
          while (idx >= 0) {
            sbpol.delete(idx, idx + (enclosure == null ? 0 : enclosure.length()));
            idx = sbpol.indexOf(enclosure + enclosure);
          }
          pol = sbpol.toString();
        }

        //  replace the escaped enclosures with enclosures...
        if (containsEscapedEnclosures) {
          String replace = escapeCharacter + enclosure;
          String replaceWith = enclosure;

          pol = Const.replace(pol, replace, replaceWith);
        }

        //replace the escaped separators with separators...
        if (containsEscapedSeparators) {
          String replace = escapeCharacter + delimiter;
          String replaceWith = delimiter;

          pol = Const.replace(pol, replace, replaceWith);
        }

        // Now add pol to the strings found!
        strings.add(pol);

        pos = next + delimiter.length();
      }
      if (pos == length) {
        if (log.isRowLevel()) {
          log.logRowlevel(BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
              BaseMessages.getString(PKG, "CsvInput.Log.EndOfEmptyLineFound"));
        }
        strings.add(""); //$NON-NLS-1$
      }
    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "CsvInput.Log.Error.ErrorConvertingLine", e.toString()),
          e); //$NON-NLS-1$
    }

    return strings.toArray(new String[strings.size()]);
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
    setOutputDone();
    return false;
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
    if(null != maxColumns) {
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

        if (Const.isEmpty(filename)) {
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