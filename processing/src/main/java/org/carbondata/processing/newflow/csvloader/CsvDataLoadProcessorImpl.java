package org.carbondata.processing.newflow.csvloader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.load.BlockDetails;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.newflow.DataField;
import org.carbondata.processing.newflow.DataLoadProcessorStep;

public class CsvDataLoadProcessorImpl implements DataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CsvDataLoadProcessorImpl.class.getName());

  private List<List<BlockDetails>> threadBlockList = new ArrayList<>();

  private ExecutorService exec;

  public CsvDataLoadProcessorImpl(BlockDetails[] blockDetails) {

    // TODO: move this logic to CarbonProperties
    int numberOfNodes;
    try {
      numberOfNodes = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException exc) {
      numberOfNodes = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }

    if (numberOfNodes > blockDetails.length) {
      numberOfNodes = blockDetails.length;
    }

    //new the empty lists
    for (int pos = 0; pos < numberOfNodes; pos++) {
      threadBlockList.add(new ArrayList<BlockDetails>());
    }

    //block balance to every thread
    for (int pos = 0; pos < blockDetails.length; ) {
      for (int threadNum = 0; threadNum < numberOfNodes; threadNum++) {
        if (pos < blockDetails.length) {
          threadBlockList.get(threadNum).add(blockDetails[pos++]);
        }
      }
    }
    exec = Executors.newFixedThreadPool(numberOfNodes);

  }

  @Override public DataField[] getOutput() {
    return new DataField[0];
  }

  @Override public List<Iterator<Object[]>> execute() {


    return null;
  }

  private void createIterator(List<BlockDetails> blockList) {
    /**
     * 1) Skip first line always for every block other than first block in file
     * 2) Always read line by line
     * 3) Always read first line of next block and break
     */
    BlockDataHandler blockDataHandler = new BlockDataHandler();
    Object[] out = null;
    int processingBlockIndex = 0;
    while (processingBlockIndex < blockList.size()) {
      //get block to handle
      BlockDetails blockDetails = blockList.get(processingBlockIndex);
      //move to next block for next time to handle
      processingBlockIndex++;
      //open file and seek to block offset
      if (blockDataHandler.openFile(meta, data, getTransMeta(), blockDetails)) {
        while (true) {
          //scan the block data
          if (blockDetails.getBlockOffset() != 0
              && blockDataHandler.isNeedToSkipFirstLineInBlock) {
            //move cursor to the block offset
            blockDataHandler
                .initializeFileReader(blockDetails.getFilePath(), blockDetails.getBlockOffset());
            //skip first line
            blockDataHandler.readOneRow(false);
            blockDataHandler.isNeedToSkipFirstLineInBlock = false;
          }
          if (blockDataHandler.currentOffset <= blockDetails.getBlockLength()) {
            out = blockDataHandler.readOneRow(true);
            if (out != null) {
              if (!CarbonCommonConstants.BLANK_LINE_FLAG.equals(out[0])) {
                synchronized (putRowLock) {
                  putRow(data.outputRowMeta, out);
                }
              } else {
                LOGGER.warn("Found a bad record, it is a blank line. Skip it !");
              }
            }
          }
          if (out == null || blockDetails.getBlockLength() < blockDataHandler.currentOffset) {
            //over the block size or end of the block
            break;
          }
        }
      }
    }
    //close the last inputstream
    if (blockDataHandler.bufferedInputStream != null) {
      blockDataHandler.bufferedInputStream.close();
    }
  } catch (RuntimeException e) {
    throw e;
  } catch (Exception e) {
    throw new RuntimeException(e);
  }
  }

}
