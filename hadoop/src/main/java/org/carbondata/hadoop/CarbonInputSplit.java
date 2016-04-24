package org.carbondata.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Venkata Ramana G on 1/4/16.
 */
public class CarbonInputSplit extends FileSplit implements Writable {

    private int segmentId;

    public CarbonInputSplit(int segmentId, Path path, long start, long length, String[] locations) {
        super(path, start, length, locations);
        this.segmentId = segmentId;
    }

    public static CarbonInputSplit from(int segmentId, FileSplit split) throws IOException {
        return new CarbonInputSplit(segmentId, split.getPath(), split.getStart(),
                split.getLength(), split.getLocations());
    }
}
