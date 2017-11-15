package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;

import java.io.IOException;
import java.util.BitSet;

public class LuceneFilterExecuter implements FilterExecuter {
    /**
     * API will apply filter based on resolver instance
     *
     * @param blocksChunkHolder
     * @param useBitsetPipeLine
     * @return
     * @throws FilterUnsupportedException
     */
    public BitSetGroup applyFilter(BlocksChunkHolder blocksChunkHolder, boolean useBitsetPipeLine) throws FilterUnsupportedException, IOException {
        return null;
    }

    /**
     * API will verify whether the block can be shortlisted based on block
     * max and min key.
     *
     * @param blockMaxValue
     * @param blockMinValue
     * @return BitSet
     */
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
        return null;
    }

    /**
     * It just reads necessary block for filter executor, it does not uncompress the data.
     *
     * @param blockChunkHolder
     */
    public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {

    }
}
