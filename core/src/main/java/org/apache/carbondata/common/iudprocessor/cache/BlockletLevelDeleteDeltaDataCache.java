package org.apache.carbondata.common.iudprocessor.cache;

import org.roaringbitmap.RoaringBitmap;

/**
 * Created by S71955 on 06-10-2016.
 */
public class BlockletLevelDeleteDeltaDataCache {
  private RoaringBitmap deleteDelataDataCache;
  private String timeStamp;

  public BlockletLevelDeleteDeltaDataCache(int[] deleteDeltaFileData, String timeStamp) {
    deleteDelataDataCache = RoaringBitmap.bitmapOf(deleteDeltaFileData);
    this.timeStamp=timeStamp;
  }

  public boolean contains(int key) {
    return deleteDelataDataCache.contains(key);
  }

  public int getSize() {
    return deleteDelataDataCache.getCardinality();
  }

  public String getCacheTimeStamp() {
    return timeStamp;
  }
}

