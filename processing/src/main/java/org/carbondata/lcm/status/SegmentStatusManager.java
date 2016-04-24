package org.carbondata.lcm.status;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperations;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonCoreLogEvent;

/**
 * Manages Load/Segment status
 */
public class SegmentStatusManager {

    private static final Log LOG = LogFactory.getLog(SegmentStatusManager.class);

    AbsoluteTableIdentifier absoluteTableIdentifier;

    public SegmentStatusManager(AbsoluteTableIdentifier absoluteTableIdentifier) {
        this.absoluteTableIdentifier = absoluteTableIdentifier;
    }

    public class ValidSegmentsInfo {
        public List<String> listOfValidSegments;
        public List<String> listOfValidUpdatedSegments;

        public ValidSegmentsInfo(List<String> listOfValidSegments,
                List<String> listOfValidUpdatedSegments) {
            this.listOfValidSegments = listOfValidSegments;
            this.listOfValidUpdatedSegments = listOfValidUpdatedSegments;
        }
    }

    public ValidSegmentsInfo getValidSegments()
            throws IOException {

        // @TODO: move reading LoadStatus file to separate class
        List<String> listOfValidSegments = new ArrayList<String>(10);
        List<String> listOfValidUpdatedSegments = new ArrayList<String>(10);
        CarbonTablePath carbonTablePath = CarbonStorePath
                .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
                        absoluteTableIdentifier.getCarbonTableIdentifier());
        String dataPath =carbonTablePath.getTableStatusFilePath();

        DataInputStream dataInputStream = null;
        Gson gsonObjectToRead = new Gson();
        AtomicFileOperations fileOperation =
                new AtomicFileOperationsImpl(dataPath, FileFactory.getFileType(dataPath));
        LoadMetadataDetails[] loadFolderDetailsArray;
        try {
            if (FileFactory.isFileExist(dataPath, FileFactory.getFileType(dataPath))) {

                dataInputStream = fileOperation.openForRead();

                BufferedReader buffReader =
                        new BufferedReader(new InputStreamReader(dataInputStream, "UTF-8"));

                loadFolderDetailsArray =
                        gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
                //just directly iterate Array
                List<LoadMetadataDetails> loadFolderDetails = Arrays.asList(loadFolderDetailsArray);

                for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails) {
                    if (CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
                            .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
                            || CarbonCommonConstants.MARKED_FOR_UPDATE
                            .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
                            || CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
                            .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
                        // check for merged loads.
                        if (null != loadMetadataDetails.getMergedLoadName()) {

                            if (!listOfValidSegments
                                    .contains(loadMetadataDetails.getMergedLoadName())) {
                                listOfValidSegments.add(loadMetadataDetails.getMergedLoadName());
                            }
                            // if merged load is updated then put it in updated list
                            if (CarbonCommonConstants.MARKED_FOR_UPDATE
                                    .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
                                listOfValidUpdatedSegments
                                        .add(loadMetadataDetails.getMergedLoadName());
                            }
                            continue;
                        }

                        if (CarbonCommonConstants.MARKED_FOR_UPDATE
                                .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {

                            listOfValidUpdatedSegments.add(loadMetadataDetails.getLoadName());
                        }
                        listOfValidSegments.add(loadMetadataDetails.getLoadName());

                    }
                }
            }
            else {
                loadFolderDetailsArray = new LoadMetadataDetails[0];
            }
        } catch (IOException e) {
            LOG.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e);
            throw e;
        } finally {
            try {

                if (null != dataInputStream) {
                    dataInputStream.close();
                }
            } catch (Exception e) {
                LOG.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e);
                throw e;
            }

        }
        return new ValidSegmentsInfo(listOfValidSegments, listOfValidUpdatedSegments);
    }

}
