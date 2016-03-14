package com.huawei.unibi.molap.factreader.columnar;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.factreader.FactReaderInfo;
import com.huawei.unibi.molap.factreader.MolapSurrogateTupleHolder;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.util.MolapSliceAndFiles;

public class ColumnarFactReaderIterator implements MolapIterator<Object[]>
{
    /**
     * measureCount
     */
    private int measureCount;

    /**
     * slice model list
     */
    private List<MolapIterator<MolapSurrogateTupleHolder>> sliceHolderList;
    
    /**
     * slice holder
     */
    private MolapIterator<MolapSurrogateTupleHolder> molapSliceTupleIterator;
    
    /**
     * output record size
     */
    private int outRecordSize;
    
    /**
     * counter
     */
    private int counter;
    
    /**
     * totalSize
     */
    private int totalSize;
    
    
    /**
     * FactReaderIterator iterator
     * @param measureCount
     * @param dimLens
     * @param schemaName
     * @param cubeName
     * @param tableName
     * @param isReadOnInProgress
     */
    public ColumnarFactReaderIterator(FactReaderInfo factItreatorInfo,List<MolapSliceAndFiles> sliceFactFilesList)
    {
        this.measureCount = factItreatorInfo.getMeasureCount();
        int mdkeyLength=KeyGeneratorFactory.getKeyGenerator(factItreatorInfo.getDimLens()).getKeySizeInBytes();
        this.outRecordSize=measureCount+1;
        this.sliceHolderList = new ArrayList<MolapIterator<MolapSurrogateTupleHolder>>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE); 
        MolapIterator<MolapSurrogateTupleHolder> molapSliceTupleItr = null;
        //CHECKSTYLE:OFF    Approval No:Approval-367
        for(MolapSliceAndFiles  sf: sliceFactFilesList)
        {//CHECKSTYLE:ON
            molapSliceTupleItr = new MolapColumnarLeafTupleIterator(sf
                    .getPath(), sf.getSliceFactFilesList(), factItreatorInfo,mdkeyLength);
            this.sliceHolderList.add(molapSliceTupleItr);
        }
        totalSize=this.sliceHolderList.size();
        if(totalSize>0)
        {
            this.molapSliceTupleIterator= this.sliceHolderList.get(counter++);
        }
    }

    /**
     * check whether more tuples are present
     */
    @Override
    public boolean hasNext()
    {
        if(null!=molapSliceTupleIterator && molapSliceTupleIterator.hasNext())
        {
            return true;
        }
        else
        {
            if(counter<totalSize)
            {
                molapSliceTupleIterator=sliceHolderList.get(counter++);
                return true;
            }
        }
        return false;
    }

    /**
     * below method will be used to get the tuple
     */
    @Override 
    public Object[] next()
    {
        Object[] rowVal = new Object[this.outRecordSize];
        MolapSurrogateTupleHolder next=molapSliceTupleIterator.next();
        Object[] measures = next.getMeasures();
        System.arraycopy(measures, 0, rowVal, 0, measures.length);
        rowVal[measures.length]=next.getMdKey();
        return rowVal;
    }
}
