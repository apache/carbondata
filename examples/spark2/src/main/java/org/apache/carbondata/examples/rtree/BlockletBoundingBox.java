package org.apache.carbondata.examples.rtree;

public class BlockletBoundingBox {
    String directoryPath;
    Integer blockletId;
    double lowx, lowy, highx, highy;

    public BlockletBoundingBox(double[] low, double[] high, int blockletId, String directoryPath) {
        assert(low.length == 2 && low.length == high.length);
        this.directoryPath = directoryPath;
        this.blockletId = blockletId;
        this.lowx = low[0];
        this.lowy = low[1];
        this.highx = high[1];
        this.highy = high[1];
    }

    public Integer getBlockletId() {
        return blockletId;
    }
}
