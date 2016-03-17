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

package com.huawei.unibi.molap.engine.querystats;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * This class will write query statistics to store location
 * 
 * @author A00902717
 *
 */
public class BinaryQueryStore implements QueryStore
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(BinaryQueryStore.class.getName());

    /**
     * write query stats to store location
     * 
     * @param queryDetail
     */
    public  void logQuery(QueryDetail queryDetail)
    {

        String queryStatsPath = getQueryStatsPath(queryDetail.getMetaPath(),queryDetail.getSchemaName(), queryDetail.getCubeName(),
                queryDetail.getFactTableName());
        if(!createDirectory(queryStatsPath))
        {
            return;
        }

        FileType fileType = FileFactory.getFileType(queryStatsPath);
        DataOutputStream dos = null;
        String queryStatsfile=queryStatsPath + File.separator
                + Preference.QUERYSTATS_FILE_NAME;
        try
        {
            if(FileFactory.isFileExist(queryStatsfile, fileType))
            {
                dos=FileFactory.getDataOutputStreamUsingAppend(queryStatsfile, fileType);
            }
            else
            {
                dos = FileFactory.getDataOutputStream(queryStatsfile, fileType);
            }
            writeQueryToFile(queryDetail, dos);            
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Error logging query for cube:"+queryDetail.getCubeName(),e);

        }
        finally
        {
            MolapUtil.closeStreams(dos);
        }
    }

    
    private synchronized void writeQueryToFile(QueryDetail queryDetail, DataOutputStream dos) throws IOException
    {
        // write dimension ordinals
        int[] dimOrdinals = queryDetail.getDimOrdinals();
        dos.writeInt(dimOrdinals.length);
        for(int i = 0;i < dimOrdinals.length;i++)
        {
            dos.writeInt(dimOrdinals[i]);
        }
        // put query string
        /*byte[] query = queryDetail.getQuery().getBytes(Charset.defaultCharset());
        dos.writeInt(query.length);
        dos.write(query);*/

        // putting executiontime
        dos.writeLong(queryDetail.getTotalExecutionTime());

        // putting recordsize
        dos.writeLong(queryDetail.getRecordSize());

        // query time
        dos.writeLong(queryDetail.getQueryStartTime());

        // no of nodes scanned
        dos.writeLong(queryDetail.getNoOfNodesScanned());

        // no of rows scanned
        dos.writeLong(queryDetail.getNoOfRowsScanned());

        // if query is groupby
        dos.writeBoolean(queryDetail.isGroupBy());

        // if query is filter query
        dos.writeBoolean(queryDetail.isFilterQuery());
        
        //if query has limit parameter
        dos.writeBoolean(queryDetail.isLimitPassed());
        //flush to file
        dos.flush();
    }

    /**
     * read queryDetails from store location
     * 
     * @param queryStatsPath
     * @return
     */
    public QueryDetail[] readQueryDetail(String queryStatsPath)
    {
      //  QueryNormalizer queryNormalizer = new QueryNormalizer();
        List<QueryDetail> queryDetails=new ArrayList<QueryDetail>(1000);
        DataInputStream dis = null;
        try
        {
            FileType fileType = FileFactory.getFileType(queryStatsPath);
            dis = FileFactory.getDataInputStream(queryStatsPath, fileType);

            // no of dimensions for current query
            int noOfDimension = dis.readInt();

            while(noOfDimension != -1)
            {
                QueryDetail queryDetail = new QueryDetail();
                // read all dimensions
                int[] dimOrdinals = new int[noOfDimension];
                for(int i = 0;i < noOfDimension;i++)
                {
                    dimOrdinals[i] = dis.readInt();
                }
                queryDetail.setDimOrdinals(dimOrdinals);
                // read query string
               /* byte[] query = new byte[dis.readInt()];
                dis.read(query)*/
                //queryDetail.setQuery(new String(query,Charset.defaultCharset()));
                // query execution time
                long executionTime = dis.readLong();
                queryDetail.setTotalExecutionTime(executionTime);

                // query result size
                long recordSize = dis.readLong();
                queryDetail.setRecordSize(recordSize);

                // query time
                long queryTime = dis.readLong();
                queryDetail.setQueryStartTime(queryTime);

                // no of nodes scanned
                long noOfNodesScanned = dis.readLong();
                queryDetail.setNoOfNodesScanned(noOfNodesScanned);

                // no of rows scanned
                long noOfRowsScanned = dis.readLong();
                queryDetail.setNumberOfRowsScanned(noOfRowsScanned);

                // read if query is group by
                boolean isGroupBy = dis.readBoolean();
                queryDetail.setGroupBy(isGroupBy);
                
                //read if query is filter query
                boolean isFilterQuery = dis.readBoolean();
                queryDetail.setFilterQuery(isFilterQuery);

                //read if query has limit parameter
                boolean isLimitPassed = dis.readBoolean();
                queryDetail.setLimitPassed(isLimitPassed);
               
                
                queryDetails.add(queryDetail);
                
                // no of dimensions for next query
                noOfDimension = dis.readInt();

            }          

        }
        catch(Exception e)
        {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Error reading querystats:"+queryStatsPath,e);
        }
        finally
        {
             MolapUtil.closeStreams(dis);
        }
        
        return queryDetails.toArray(new QueryDetail[queryDetails.size()]);
    }

    /**
     * Delete expired query from query stats file
     * 
     * @param queryDetails
     */
    public void writeQueryToFile(List<QueryDetail> queryDetails, String queryStatsPath)
    {
        DataOutputStream dos = null;
        try
        {
            String tempQueryStatsPath = queryStatsPath + "_temp";
            FileType fileType = FileFactory.getFileType(queryStatsPath);

            dos = FileFactory.getDataOutputStream(tempQueryStatsPath, fileType, 10240, true);

            for(QueryDetail queryDetail : queryDetails)
            {
                writeQueryToFile(queryDetail, dos);
            }
            dos.close();
            // renaming temporary file to original
            if(FileFactory.getMolapFile(queryStatsPath, fileType).delete())
            {
                if(!FileFactory.getMolapFile(tempQueryStatsPath, fileType).renameTo(queryStatsPath))
                {
                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                            "Error renaming querystats_temp to querystats:"+queryStatsPath);
                }
            }
        }
        catch(Exception e)
        {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Error rewriting querystats:"+queryStatsPath,e);
        }
        finally
        {
             MolapUtil.closeStreams(dos);
        }

    }

    /**
     * store location path to store query stats
     * 
     * @param queryDetail
     * @return
     */
    private static String getQueryStatsPath(String metaPath,String schemaName, String cubeName, String factTableName)
    {
        StringBuffer queryStatsPath = new StringBuffer(metaPath);
        queryStatsPath.append(File.separator).append(Preference.AGGREGATE_STORE_DIR);
        return queryStatsPath.toString();
    }

    private static boolean createDirectory(String path)
    {
        FileFactory.FileType fileType = FileFactory.getFileType(path);
        try
        {
            if(!FileFactory.isFileExist(path, fileType, false))
            {
                if(!FileFactory.mkdirs(path, fileType))
                {
                    return false;
                }
            }
        }
        catch(Exception e)
        {
            return false;
        }
        return true;
    }
    
   

    /**
     * Deleting all query stats for given schemaname and cubename
     * 
     * @param schemaName
     * @param cubeName
     * @return
     */
   /* public boolean deleteQueryStats(String dataPath,String schemaName, String cubeName)
    {
        String queryStatsPath = getQueryStatsPath(dataPath,schemaName, cubeName, null);
        FileFactory.FileType fileType = FileFactory.getFileType(queryStatsPath);
        try
        {
            if(!FileFactory.isFileExist(queryStatsPath, fileType))
            {
                return true;
            }    
        }
        catch(Exception e)
        {
            
        }
        
        MolapFile cubeFile = FileFactory.getMolapFile(queryStatsPath, fileType);
        MolapFile[] tableFiles = cubeFile.listFiles();

        if(null==tableFiles)
        {
            cubeFile.delete();
            return true;
        }
        for(MolapFile table : tableFiles)
        {
            MolapFile[] stats = table.listFiles();
            for(MolapFile stat : stats)
            {
                stat.delete();
            }
            table.delete();
        }
        return cubeFile.delete();
    }*/

}
