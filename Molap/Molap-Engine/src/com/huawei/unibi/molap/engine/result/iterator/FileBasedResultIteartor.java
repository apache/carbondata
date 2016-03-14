package com.huawei.unibi.molap.engine.result.iterator;

import java.io.IOException;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.reader.QueryDataFileReader;
import com.huawei.unibi.molap.engine.reader.exception.ResultReaderException;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : FileBasedResultIteartor.java
 * Description   : provides the iterator over the leaf node and return the query result.
 * Class Version  : 1.0
 */
public class FileBasedResultIteartor implements MolapIterator<QueryResult>
{
    /**
     * leafNodeInfos
     */
    private List<LeafNodeInfo> leafNodeInfos;

    private int counter;

    private QueryDataFileReader molapQueryDataFileReader;
    
    private boolean hasNext;
    
    private static final LogService LOGGER = LogServiceFactory.getLogService(FileBasedResultIteartor.class.getName());

    public FileBasedResultIteartor(String path, DataProcessorInfo info)
    {
        readLeafNodeInfo(path, info);
        molapQueryDataFileReader = new QueryDataFileReader(path, info);
    }

    private void readLeafNodeInfo(String path, DataProcessorInfo info)
    {
        MolapFile molapFile = FileFactory.getMolapFile(path, FileFactory.getFileType(path));
        try
        {
            if(FileFactory.isFileExist(path, FileFactory.getFileType(path)))
            {
                leafNodeInfos = MolapUtil.getLeafNodeInfo(molapFile, info.getAggType().length, info
                        .getKeySize());
            }
            else
            {
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "file doesnot exist "+path);
            }
            if(leafNodeInfos.size()>0)
            {
                hasNext=true;
            }
        }
        
        catch(IOException e)
        {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e.getMessage());
        }
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public QueryResult next()
    {
        QueryResult prepareResultFromFile = null;
        try
        {
            prepareResultFromFile = molapQueryDataFileReader.prepareResultFromFile(leafNodeInfos.get(counter));
        }
        catch(ResultReaderException e)
        {
            molapQueryDataFileReader.close();
        }
        counter++;
        if(counter>=leafNodeInfos.size())
        {
            hasNext=false;
            molapQueryDataFileReader.close();
        }
        return prepareResultFromFile;
    }

}
