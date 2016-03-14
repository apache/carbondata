/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.fileperations;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author R00903928
 *
 */
public interface AtomicFileOperations
{

    /**
     * 
     * @return
     * @throws IOException
     */
    DataInputStream openForRead() throws IOException;
    
    /**
     * @throws IOException 
     * 
     */
    void close() throws IOException;

    /**
     * 
     * @param operation
     * @return
     * @throws IOException
     */
    DataOutputStream openForWrite(FileWriteOperation operation) throws IOException;
}
