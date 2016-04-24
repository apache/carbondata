package org.carbondata.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.carbondata.core.carbon.path.CarbonTablePath;

/** Filters to accept valid carbon data Files only
 * @author Venkata Ramana G on 1/4/16.
 */
public class CarbonPathFilter implements PathFilter{

    // update extension which should be picked
    private final String validUpdateTimestamp;

    /**
     * @param validUpdateTimestamp update extension which should be picked
     */
    public CarbonPathFilter(String validUpdateTimestamp) {
        this.validUpdateTimestamp = validUpdateTimestamp;
    }

    @Override public boolean accept(Path path) {
        String updateTimeStamp = CarbonTablePath.DataFileUtil.getUpdateTimeStamp(path.getName());
        if (updateTimeStamp.equals(validUpdateTimestamp))
            return true;
        else
            return false;
    }
}
