/**
 * 
 */
package com.huawei.unibi.molap.engine.util;

import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.engine.datastorage.streams.impl.FileDataInputStream;
import com.huawei.unibi.molap.engine.datastorage.streams.impl.HDFSFileDataInputStream;

/**
 * @author R00900208
 *
 */
public final class MolapDataInputStreamFactory
{
    private MolapDataInputStreamFactory()
    {
        
    }
    public static DataInputStream getDataInputStream(String filesLocation, int mdkeysize, int msrCount,
            boolean hasFactCount, String persistenceFileLocation, String tableName,FileType fileType)
    {
        switch(fileType)
        {
        case LOCAL : 
            return new FileDataInputStream(filesLocation, mdkeysize, msrCount, hasFactCount, persistenceFileLocation, tableName);
        case HDFS : 
            return new HDFSFileDataInputStream(filesLocation, mdkeysize, msrCount, hasFactCount, persistenceFileLocation, tableName);
        default :
            return new FileDataInputStream(filesLocation, mdkeysize, msrCount, hasFactCount, persistenceFileLocation, tableName);
        }
    } 
}
