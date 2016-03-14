/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.fileperations;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * @author R00903928
 *
 */
public class AtomicFileOperationsImpl implements AtomicFileOperations
{
    
    private String filePath;
    
    private FileType fileType;
    
    private String tempWriteFilePath;
    
    private DataOutputStream dataOutStream;
    
    /**
     * 
     * @param filePath
     * @param fileType
     */
    public AtomicFileOperationsImpl(String filePath , FileType fileType)
    {
        this.filePath = filePath;
        
        this.fileType = fileType;
    }
    

    /**
     * 
     * @return DataInputStream
     * @throws IOException
     */
    @Override
    public DataInputStream openForRead() throws IOException
    {
        return FileFactory.getDataInputStream(filePath, fileType);
    }

    /**
     * 
     * @param operation
     * @return DataOutputStream
     * @throws IOException
     */
    @Override
    public DataOutputStream openForWrite(FileWriteOperation operation) throws IOException
    {

        filePath = filePath.replace("\\", "/");

        tempWriteFilePath = filePath + MolapCommonConstants.TEMPWRITEFILEEXTENSION;

        if(FileFactory.isFileExist(tempWriteFilePath, fileType))
        {
            FileFactory.getMolapFile(tempWriteFilePath, fileType).delete();
        }

        FileFactory.createNewFile(tempWriteFilePath, fileType);

        dataOutStream =  FileFactory.getDataOutputStream(tempWriteFilePath, fileType);
        
        return dataOutStream;

    }

    /* (non-Javadoc)
     * @see com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperations#close()
     */
    @Override
    public void close() throws IOException
    {
     
        if(null != dataOutStream)
        {
            dataOutStream.close();

            MolapFile tempFile = FileFactory.getMolapFile(tempWriteFilePath, fileType);

            if(!tempFile.renameForce(filePath))
            {
                throw new IOException("temporary file renaming failed");
            }
        }
                
    }

}
