package org.carbondata.core.reader;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDictionaryMetadata;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class CarbonDictionaryMetadataReader {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonDictionaryMetadataReader.class.getName());

    /**
     * This method will read the dictionary metadata file and return the last segment entry detail
     *
     * @param metadataFilePath
     * @param oneSegmentEntryLength
     * @return
     */
    public static CarbonDictionaryMetadata readAndGetDictionaryMetadataForLastSegment(
            String metadataFilePath, int oneSegmentEntryLength) {
        DataInputStream dataInputStream = null;
        CarbonDictionaryMetadata dictionaryMetadata = null;
        try {
            FileFactory.FileType fileType = FileFactory.getFileType(metadataFilePath);
            CarbonFile carbonFile = FileFactory.getCarbonFile(metadataFilePath, fileType);
            int fileSize = (int) carbonFile.getSize();
            byte[] previousSegmentDetails = new byte[oneSegmentEntryLength];
            int byteOffsetToSkip = fileSize - oneSegmentEntryLength;
            dataInputStream = FileFactory.getDataInputStream(metadataFilePath, fileType);
            // skip the bytes to read only the last segment entry
            dataInputStream.skip(byteOffsetToSkip);
            LOGGER.debug(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "Bytes skipped while reading dictionary metadata :: " + byteOffsetToSkip);
            dataInputStream.read(previousSegmentDetails, 0, oneSegmentEntryLength);
            CarbonUtil.closeStreams(dataInputStream);
            ByteBuffer byteBuffer = ByteBuffer.wrap(previousSegmentDetails);
            byteBuffer.rewind();
            dictionaryMetadata = getDictionaryMetadataObjectForLastSegment(byteBuffer);
            LOGGER.debug(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "completed reading data for last segment");
        } catch (IOException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
        } finally {
            CarbonUtil.closeStreams(dataInputStream);
        }
        return dictionaryMetadata;
    }

    /**
     * This method will create a dictionary metadata object for one segment from bytebuffer
     *
     * @param byteBuffer
     * @return
     */
    private static CarbonDictionaryMetadata getDictionaryMetadataObjectForLastSegment(
            ByteBuffer byteBuffer) {
        int segmentId = byteBuffer.getInt();
        int min = byteBuffer.getInt();
        int max = byteBuffer.getInt();
        long offset = byteBuffer.getLong();
        return new CarbonDictionaryMetadata(segmentId, min, max, offset);
    }

}
