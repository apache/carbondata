package com.huawei.unibi.molap.engine.columnar.keyvalue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;



public class NonFilterScanResult extends AbstractColumnarScanResult
{
    public NonFilterScanResult(int keySize,int[] selectedDimensionIndex)
    {
        super(keySize,selectedDimensionIndex);
    }
    public double getNormalMeasureValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableDoubleValueByIndex(currentRow);
    }
    public byte[] getCustomMeasureValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableByteArrayValueByIndex(currentRow);
    }
    
    public byte[] getKeyArray(ByteArrayWrapper keyVal)
    {
        ++currentRow;
        return getKeyArray(++sourcePosition,keyVal);
    }

    public List<byte[]> getKeyArrayWithComplexTypes(Map<Integer, GenericQueryType> complexQueryDims)
    {
        ++currentRow;
        return getKeyArrayWithComplexTypes(++sourcePosition, complexQueryDims);
    }
    
    
    
    public int getDimDataForAgg(int dimOrdinal)
    {
        return getSurrogateKey(currentRow, dimOrdinal);
    }
    @Override
    public byte[] getKeyArray()
    {
        ++currentRow;
        return getKeyArray(++sourcePosition,null);
    }
    @Override
    public byte[] getHighCardinalityDimDataForAgg(Dimension dimension)
    {
        
            ColumnarKeyStoreMetadata columnarKeyStoreMetadata = columnarKeyStoreDataHolder[dimension.getOrdinal()]
                    .getColumnarKeyStoreMetadata();
            if(null != columnarKeyStoreMetadata.getMapOfColumnarKeyBlockDataForDirectSurroagtes())
            {
                Map<Integer, byte[]> mapOfDirectSurrogates = columnarKeyStoreMetadata
                        .getMapOfColumnarKeyBlockDataForDirectSurroagtes();
                if(null==columnarKeyStoreMetadata.getColumnReverseIndex())
                {
                    return mapOfDirectSurrogates.get(currentRow);
                }
                return mapOfDirectSurrogates.get(columnarKeyStoreMetadata.getColumnReverseIndex()[currentRow]);
            }
            return null;
            // TODO Auto-generated method stub

        

    }
   
    

    @Override
    public void getComplexDimDataForAgg(GenericQueryType complexType, DataOutputStream dataOutputStream)
            throws IOException
    {
        getComplexSurrogateKey(currentRow, complexType, dataOutputStream);
    }
}
