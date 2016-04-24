package org.carbondata.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author Venkata Ramana G on 1/4/16.
 */
public class CarbonPartitionedRawDataInputSpCarbonPartitionedRawDataInputSplitlit extends InputSplit implements Writable {

    public static CarbonRawDataInputSplit from(FileSplit split) throws IOException {
        return new CarbonRawDataInputSplit(split.getPath(), split.getStart(),
                split.getLength(), split.getLocations());
    }

    @Override public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }
}
