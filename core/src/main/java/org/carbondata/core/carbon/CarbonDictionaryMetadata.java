package org.carbondata.core.carbon;

public class CarbonDictionaryMetadata {

    /**
     * segment id
     */
    private int segmentId;

    /**
     * min value for a segment
     */
    private int min;

    /**
     * max value for a segment
     */
    private int max;

    /**
     * dictionary file offset for a segment
     */
    private long offset;

    /**
     * @param segmentId
     * @param min
     * @param max
     * @param offset
     */
    public CarbonDictionaryMetadata(int segmentId, int min, int max, long offset) {
        this.segmentId = segmentId;
        this.min = min;
        this.max = max;
        this.offset = offset;
    }

    /**
     * @return
     */
    public int getSegmentId() {
        return segmentId;
    }

    /**
     * @return
     */
    public int getMin() {
        return min;
    }

    /**
     * @return
     */
    public int getMax() {
        return max;
    }

    /**
     * @return
     */
    public long getOffset() {
        return offset;
    }

}
