package org.apache.carbondata.common.iudprocessor.iuddata;

import org.apache.carbondata.common.iudprocessor.cache.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.updatestatus.SegmentUpdateStatusManager;

public class BlockletDeleteDeltaCacheLoader implements DeleteDeltaCacheLoaderIntf {
  private String blockletID;
  private DataRefNode blockletNode;
  private AbsoluteTableIdentifier absoluteIdentifier;
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockletDeleteDeltaCacheLoader.class.getName());

  public BlockletDeleteDeltaCacheLoader(String blockletID,
                                        DataRefNode blockletNode, AbsoluteTableIdentifier absoluteIdentifier) {
    this.blockletID = blockletID;
    this.blockletNode = blockletNode;
    this.absoluteIdentifier= absoluteIdentifier;
  }

  /**
   * This method will load the delete delta cache based on blocklet id of particular block with
   * the help of SegmentUpdateStatusManager.
   */
  public void loadDeleteDeltaFileDataToCache() {
    SegmentUpdateStatusManager segmentUpdateStatusManager =
        new SegmentUpdateStatusManager(absoluteIdentifier);
    int[] deleteDeltaFileData = null;
    BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache = null;
    if (null == blockletNode.getDeleteDeltaDataCache()) {
      try {
        deleteDeltaFileData = segmentUpdateStatusManager.getDeleteDeltaDataFromAllFiles(blockletID);
        deleteDeltaDataCache = new BlockletLevelDeleteDeltaDataCache(deleteDeltaFileData,
            segmentUpdateStatusManager.getTimestampForRefreshCache(blockletID, null));
      } catch (Exception e) {
        LOGGER.debug("Unable to retrieve delete delta files");
      }
    } else {
      deleteDeltaDataCache = blockletNode.getDeleteDeltaDataCache();
      // if already cache is present then validate the cache using timestamp
      String cacheTimeStamp = segmentUpdateStatusManager
          .getTimestampForRefreshCache(blockletID, deleteDeltaDataCache.getCacheTimeStamp());
      if (null != cacheTimeStamp) {
        try {
          deleteDeltaFileData =
              segmentUpdateStatusManager.getDeleteDeltaDataFromAllFiles(blockletID);
          deleteDeltaDataCache = new BlockletLevelDeleteDeltaDataCache(deleteDeltaFileData,
              segmentUpdateStatusManager.getTimestampForRefreshCache(blockletID, cacheTimeStamp));
        } catch (Exception e) {
          LOGGER.debug("Unable to retrieve delete delta files");
        }
      }
    }
    blockletNode.setDeleteDeltaDataCache(deleteDeltaDataCache);
  }
}
