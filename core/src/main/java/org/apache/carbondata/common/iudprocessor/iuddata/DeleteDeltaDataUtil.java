package org.apache.carbondata.common.iudprocessor.iuddata;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.updatestatus.SegmentStatusManager;

/**
 * Created by S71955 on 06-10-2016.
 */
public class DeleteDeltaDataUtil {

  private DeleteDeltaDataUtil() {

  }

  public static int transformDataToNumericType(String deleteDeltadata) {
    return Integer.parseInt(
        deleteDeltadata.substring(deleteDeltadata.lastIndexOf('.'), deleteDeltadata.length()));
  }

  /**
   * This method will verify whether any segment is updated or not as part of
   * delete/update query.
   * @param segmentStatusManager
   * @param tableIdentifier
   * @return boolean
   */
  public static boolean isSegmentStatusUpdated(SegmentStatusManager segmentStatusManager,
      AbsoluteTableIdentifier tableIdentifier, String segmentID) {
    CarbonTable table = CarbonMetadata.getInstance()
        .getCarbonTable(tableIdentifier.getCarbonTableIdentifier().getTableUniqueName());
    LoadMetadataDetails[] loadMetadataDetails =
        segmentStatusManager.readLoadMetadata(table.getMetaDataFilepath());
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      if (segmentID.equals(loadMetadataDetail.getLoadName()) && "TRUE"
          .equalsIgnoreCase(loadMetadataDetail.getIsDeleted())) {
        return true;
      }
    }
    return false;
  }
}