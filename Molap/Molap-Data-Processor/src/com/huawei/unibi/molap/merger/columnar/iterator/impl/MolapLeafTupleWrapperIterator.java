package com.huawei.unibi.molap.merger.columnar.iterator.impl;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.factreader.MolapSurrogateTupleHolder;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.merger.columnar.iterator.MolapDataIterator;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;

/**
 * This class is a wrapper class over MolapColumnarLeafTupleDataIterator.
 * This uses the global key gen for generating key.
 * @author R00903928
 *
 */
public class MolapLeafTupleWrapperIterator implements MolapDataIterator<MolapSurrogateTupleHolder>
{
    MolapDataIterator<MolapSurrogateTupleHolder> iterator;
    
    private KeyGenerator localKeyGen; 
    
    private KeyGenerator globalKeyGen;
    
    /**
     * logger
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapLeafTupleWrapperIterator.class.getName());
    
    public MolapLeafTupleWrapperIterator(KeyGenerator localKeyGen, KeyGenerator globalKeyGen, MolapDataIterator<MolapSurrogateTupleHolder> iterator)
    {
        this.iterator = iterator;
        this.localKeyGen = localKeyGen;
        this.globalKeyGen = globalKeyGen;
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public void fetchNextData()
    {
        iterator.fetchNextData();
    }

    @Override
    public MolapSurrogateTupleHolder getNextData()
    {
        MolapSurrogateTupleHolder nextData = iterator.getNextData();
        byte[] mdKey = nextData.getMdKey();
        long[] keyArray = localKeyGen.getKeyArray(mdKey);
        byte[] generateKey = null;
        try
        {
            generateKey = globalKeyGen.generateKey(keyArray);
        }
        catch(KeyGenException e)
        {
            LOGGER.error(
                    MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG ,
                    "Error occurred :: " + e.getMessage());
        }
        nextData.setSurrogateKey(generateKey);
        return nextData;
    }
}
