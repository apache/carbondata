/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.dataprocessor.dataretention;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapFileFolderComparator;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

public final class MolapDataRetentionUtil
{

    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapDataRetentionUtil.class.getName());

    private MolapDataRetentionUtil()
    {

    }

    /**
     * This API will scan the level file and return the surrogate key for the
     * valid member
     * 
     * @param folderList
     * @param surrogateKeys
     * @param levelName
     * @param dataPeriod
     *            int
     * @throws ParseException
     */
    public static Map<Integer, Integer> getSurrogateKeyForRetentionMember(
            MolapFile memberFile, String levelName, String columnValue,
            String format, Map<Integer, Integer> mapOfSurrKeyAndAvailStatus)
    {
        DataInputStream inputStream = null;
        Date storeDateMember = null;
        Date columnValDateMember=null;
        DataInputStream inputStreamForMaxVal  = null;
        try
        {
            columnValDateMember = convertToDateObjectFromStringVal(
                    columnValue, format, true);
        }
        catch(ParseException e)
        {
            LOGGER.error(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    e, "Not able to get surrogate key for value : "
                            + columnValue);
            return mapOfSurrKeyAndAvailStatus;
        }
        try
        {
            inputStream = FileFactory.getDataInputStream(memberFile.getPath(),
                    FileFactory.getFileType(memberFile.getPath()));

            long currPosIndex = 0;
            long size = memberFile.getSize() - 4;
            int minVal=inputStream.readInt();
            int surrogateKeyIndex = minVal;
            currPosIndex += 4;
            //
            int current=0;
            // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
            boolean enableEncoding = Boolean.valueOf(MolapProperties.getInstance().getProperty(
                        MolapCommonConstants.ENABLE_BASE64_ENCODING, MolapCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
            // CHECKSTYLE:ON
			String memberName = null;
            while(currPosIndex < size)
            {
            	int len = inputStream.readInt();
                // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
                // CHECKSTYLE:ON
                currPosIndex += 4;
                byte[] rowBytes = new byte[len];
                inputStream.readFully(rowBytes);
                currPosIndex += len;
               // CHECKSTYLE:OFF Approval
                                         // No:Approval-361
                if(enableEncoding)
                {
                    memberName = new String(Base64.decodeBase64(rowBytes), Charset.defaultCharset());
                }
                else
                {
                    memberName = new String(rowBytes, Charset.defaultCharset());
                }
                int surrogateVal = surrogateKeyIndex+current;
                current++;
                try
                {
                    storeDateMember = convertToDateObjectFromStringVal(memberName,
                            format, false);

                    /*
                     * if(storeDateMember.compareTo(columnValDateMember)==0){
                     * surrogateValue=surrogateVal; isPresent=true; break; }
                     */
                }
                catch(Exception e)
                {
                    LOGGER.error(
                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            e, "Not able to get surrogate key for value : "
                                    + memberName);
                    continue;
                }
                // means date1 is before date2
				if (null != columnValDateMember && null != storeDateMember) {
					if (storeDateMember.compareTo(columnValDateMember) < 0) {
						mapOfSurrKeyAndAvailStatus.put(surrogateVal,
								surrogateVal);
					}
				}

            }

        }
        catch(IOException e)
        {
            LOGGER.error(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Not able to read level file for Populating Cache : "
                            + memberFile.getName());
            // MolapUtil.closeStreams(inputStream);

        }
        finally
        {
            MolapUtil.closeStreams(inputStream);
            MolapUtil.closeStreams(inputStreamForMaxVal);
            
        }

        // mapOfSurrKeyAndAvailStatus.put(surrogateValue, isPresent);
        return mapOfSurrKeyAndAvailStatus;
    }

    @SuppressWarnings("deprecation")
    private static Date convertToDateObjectFromStringVal(String value,
            String dateFormatVal, boolean isUserInPut) throws ParseException
    {

        if(!value.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL)){

    	Date dateToConvert = null;
        String dateFormat = MolapProperties.getInstance().getProperty(
                MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT,
                MolapCommonConstants.MOLAP_TIMESTAMP_DEFAULT_FORMAT);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        // Format the date to Strings

        if(isUserInPut && (null != dateFormatVal || !"".equals(dateFormatVal)))
        {
            SimpleDateFormat defaultDateFormat = new SimpleDateFormat(
                    dateFormatVal);
            dateToConvert = defaultDateFormat.parse(value);
            String dmy = simpleDateFormat.format(dateToConvert);
            return simpleDateFormat.parse(dmy);
        }

        return simpleDateFormat.parse(value);
        }
        return null;
    }

    /**
     * 
     * @param baseStorePath
     * @param fileNameSearchPattern
     * @return
     */
    public static MolapFile[] getFilesArray(String baseStorePath,
            final String fileNameSearchPattern)
    {
        FileType fileType = FileFactory.getFileType(baseStorePath);
        MolapFile storeFolder = FileFactory.getMolapFile(baseStorePath,
                fileType);

        MolapFile[] listFiles = storeFolder.listFiles(new MolapFileFilter()
        {

            @Override
            public boolean accept(MolapFile pathname)
            {
                if(pathname.getName().indexOf(fileNameSearchPattern) > -1
                        && !pathname.getName().endsWith(
                                MolapCommonConstants.FILE_INPROGRESS_STATUS))
                {
                    return true;
                }
                return false;
            }
        });

        return listFiles;
    }

    public static MolapFile[] getAllLoadFolderSlices(String schemaName,
            String cubeName, String tableName, String hdsfStoreLocation,
            int currentRestructNumber)
    {
        String hdfsLevelRSPath = MolapUtil.getRSPath(schemaName, cubeName,
                tableName, hdsfStoreLocation, currentRestructNumber);

        MolapFile file = FileFactory.getMolapFile(hdfsLevelRSPath + '/',
                FileFactory.getFileType(hdfsLevelRSPath));

        MolapFile[] listFiles = listFiles(file);
        if(null != listFiles)
        {
            Arrays.sort(listFiles, new MolapFileFolderComparator());
        }
        return listFiles;
    }

    /**
     * @param file
     * @return
     */
    private static MolapFile[] listFiles(MolapFile file)
    {
        MolapFile[] listFiles = file.listFiles(new MolapFileFilter()
        {
            @Override
            public boolean accept(MolapFile pathname)
            {
                return pathname.getName().startsWith(
                        MolapCommonConstants.LOAD_FOLDER)
                        && !pathname.getName().endsWith(
                                MolapCommonConstants.FILE_INPROGRESS_STATUS);
            }
        });
        return listFiles;
    }

    /**
     * 
     * @param loadFiles
     * @param loadMetadataDetails
     * @return
     */
    public static MolapFile[] excludeUnwantedLoads(MolapFile[] loadFiles,
            List<LoadMetadataDetails> loadMetadataDetails)
    {
        List<MolapFile> validLoads = new ArrayList<MolapFile>();
        
        List<String> validLoadsForRetention =  getValidLoadsForRetention(loadMetadataDetails);
        
        for(MolapFile loadFolder : loadFiles)
        {
            String loadName = loadFolder.getName().substring(loadFolder.getName().indexOf(MolapCommonConstants.LOAD_FOLDER)+MolapCommonConstants.LOAD_FOLDER.length(),loadFolder.getName().length() );
            
            if(validLoadsForRetention.contains(loadName))
            {
                validLoads.add(loadFolder);
            }
            
        }
        
        
        
        
        return validLoads.toArray(new MolapFile[validLoads.size()]);
    }

    /**
     * 
     * @param loadMetadataDetails
     * @return
     */
    private static List<String> getValidLoadsForRetention(
            List<LoadMetadataDetails> loadMetadataDetails)
    {
        List<String> validLoadNameForRetention = new ArrayList<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        for(LoadMetadataDetails loadDetail : loadMetadataDetails )
        {
            //load should not be deleted and load should not be merged.
            if(!loadDetail.getLoadStatus().equalsIgnoreCase(MolapCommonConstants.MARKED_FOR_DELETE))
            {
                if(null == loadDetail.getMergedLoadName())
                {
                    validLoadNameForRetention.add(loadDetail.getLoadName());
                }
                else
                {
                    validLoadNameForRetention.add(loadDetail.getMergedLoadName());
                }
                   
            }
            
        }
        
        return validLoadNameForRetention;
    }
}
