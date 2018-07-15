package org.apache.carbondata.core.statusmanager;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class SegmentManagerHelper {

  /**
   * This method will update the load status entry in the table status file
   */
  public static SegmentDetailVO updateFailStatusAndGetSegmentVO(String segmentId, String transaction_id) {
    SegmentDetailVO detailVO = new SegmentDetailVO();
    detailVO.setSegmentId(segmentId);
    detailVO.setLoadEndTime(System.currentTimeMillis());
    detailVO.setStatus(SegmentStatus.MARKED_FOR_DELETE.toString());
    detailVO.setTransactionId(transaction_id);
    return detailVO;
  }

  /**
   * This method will update the load status entry in the table status file
   */
  public static SegmentDetailVO updateFailStatusAndGetSegmentVO(String segmentId) {
    return updateFailStatusAndGetSegmentVO(segmentId, null);
  }

  public static SegmentDetailVO createSegmentVO(String segmentId, SegmentStatus status,  Long startTime) {
    SegmentDetailVO detailVO = new SegmentDetailVO();
    detailVO.setSegmentId(segmentId);
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    detailVO.setLoadStartTime(startTime);
    detailVO.setStatus(status.toString());
    return detailVO;
  }

  public static LoadMetadataDetails createLoadMetadataDetails(SegmentDetailVO detailVO) {
    LoadMetadataDetails details = new LoadMetadataDetails();
    return updateLoadMetadataDetails(detailVO, details);
  }

  public static LoadMetadataDetails updateLoadMetadataDetails(SegmentDetailVO detailVO,
      LoadMetadataDetails details) {
    Map<String, Object> fields = detailVO.getAllFields();
    if (fields.get(SegmentDetailVO.STATUS) != null) {
      details.setSegmentStatus(convertToSegmentStatus(detailVO.getStatus()));
    }
    if (fields.get(SegmentDetailVO.SEGMENT_ID) != null) {
      details.setLoadName(detailVO.getSegmentId());
    }
    if (fields.get(SegmentDetailVO.MODIFICATION_OR_DELETION_TIMESTAMP) != null) {
      details.setModificationOrdeletionTimesStamp(detailVO.getModificationOrDeletionTimestamp());
    }
    if (fields.get(SegmentDetailVO.LOAD_START_TIME) != null) {
      details.setLoadStartTime(detailVO.getLoadStartTime());
    }
    if (fields.get(SegmentDetailVO.IS_DELETED) != null) {
      details.setIsDeleted(String.valueOf(detailVO.getIsDeleted()));
    }
    if (fields.get(SegmentDetailVO.LOAD_END_TIME) != null) {
      details.setLoadEndTime(detailVO.getLoadEndTime());
    }
    if (fields.get(SegmentDetailVO.SEGMENT_FILE_NAME) != null) {
      details.setSegmentFile(detailVO.getSegmentFileName());
    }
    if (fields.get(SegmentDetailVO.UPDATE_DELTA_END_TIMESTAMP) != null) {
      details.setUpdateDeltaEndTimestamp(String.valueOf(detailVO.getUpdateDeltaEndTimestamp()));
    }
    if (fields.get(SegmentDetailVO.UPDATE_DELTA_START_TIMESTAMP) != null) {
      details.setUpdateDeltaStartTimestamp(String.valueOf(detailVO.getUpdateDeltaStartTimestamp()));
    }
    if (fields.get(SegmentDetailVO.UPDATE_STATUS_FILENAME) != null) {
      details.setUpdateStatusFileName(detailVO.getUpdateStatusFilename());
    }
    if (fields.get(SegmentDetailVO.DATA_SIZE) != null) {
      details.setDataSize(String.valueOf(detailVO.getDataSize()));
    }
    if (fields.get(SegmentDetailVO.FILE_FORMAT) != null) {
      details.setFileFormat(FileFormat.valueOf(detailVO.getFileFormat()));
    }
    if (fields.get(SegmentDetailVO.INDEX_SIZE) != null) {
      details.setIndexSize(String.valueOf(detailVO.getIndexSize()));
    }
    if (fields.get(SegmentDetailVO.MAJOR_COMPACTED) != null) {
      details.setMajorCompacted(detailVO.getMajorCompacted());
    }
    if (fields.get(SegmentDetailVO.MERGED_SEGMENT_IDS) != null) {
      details.setMergedLoadName(detailVO.getMergedSegmentIds());
    }
    if (fields.get(SegmentDetailVO.VISIBILITY) != null) {
      details.setVisibility(String.valueOf(detailVO.getVisibility()));
    }
    return details;
  }

  public static SegmentDetailVO convertToSegmentDetailVO(LoadMetadataDetails detail) {
    SegmentDetailVO detailVO = new SegmentDetailVO();
    detailVO.setStatus(detail.getSegmentStatus().toString());
    detailVO.setSegmentId(detail.getLoadName());
    detailVO.setModificationOrDeletionTimestamp(detail.getModificationOrdeletionTimesStamp());
    detailVO.setLoadStartTime(detail.getLoadStartTime());
    detailVO.setIsDeleted(Boolean.parseBoolean(detail.getIsDeleted()));
    detailVO.setLoadEndTime(detail.getLoadEndTime());
    detailVO.setSegmentFileName(detail.getSegmentFile());
    if (StringUtils.isNotEmpty(detail.getUpdateDeltaEndTimestamp())) {
      detailVO.setUpdateDeltaEndTimestamp(Long.parseLong(detail.getUpdateDeltaEndTimestamp()));
    }
    if (StringUtils.isNotEmpty(detail.getUpdateDeltaStartTimestamp())) {
      detailVO.setUpdateDeltaStartTimestamp(Long.parseLong(detail.getUpdateDeltaStartTimestamp()));
    }
    detailVO.setUpdateStatusFilename(detail.getDataSize());
    if (StringUtils.isNotEmpty(detail.getUpdateDeltaStartTimestamp())) {
      detailVO.setDataSize(Long.valueOf(detail.getDataSize()));
    }
    if (StringUtils.isNotEmpty(detail.getIndexSize())) {
      detailVO.setIndexSize(Long.valueOf(detail.getIndexSize()));
    }
    detailVO.setFileFormat(detail.getFileFormat().toString());
    detailVO.setMajorCompacted(detail.isMajorCompacted());
    detailVO.setMergedSegmentIds(detail.getMergedLoadName());
    detailVO.setVisibility(Boolean.valueOf(detail.getVisibility()));
    return detailVO;
  }

  private static SegmentStatus convertToSegmentStatus(String message) {
    for (SegmentStatus status : SegmentStatus.values()) {
      if (status.toString().equals(message)) {
        return status;
      }
    }
    return null;
  }
}
