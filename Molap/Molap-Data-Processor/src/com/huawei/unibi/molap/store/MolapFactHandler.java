package com.huawei.unibi.molap.store;

import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;

public interface MolapFactHandler
{
    void initialise() throws MolapDataWriterException;
    
    void addDataToStore(Object[] row) throws MolapDataWriterException;
    
    void finish() throws MolapDataWriterException;
    
    void closeHandler();
}
