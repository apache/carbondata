package com.huawei.unibi.molap.engine.columnar.keyvalue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;


import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;


public class FilterScanResult extends AbstractColumnarScanResult
{
    public FilterScanResult(int keySize, int[] selectedDimensionIndex) 
    {
        super(keySize, selectedDimensionIndex);
    }

    public double getNormalMeasureValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableDoubleValueByIndex(rowMapping[currentRow]);
    }

    public byte[] getCustomMeasureValue(int measureOrdinal)
    {
        return measureBlocks[measureOrdinal].getReadableByteArrayValueByIndex(rowMapping[currentRow]);
    }

    public byte[] getKeyArray(ByteArrayWrapper key)
    {
        ++currentRow;
        return getKeyArray(rowMapping[++sourcePosition],key);
    }
    public List<byte[]> getKeyArrayWithComplexTypes(Map<Integer, GenericQueryType> complexQueryDims)
    {
        ++currentRow;
        return getKeyArrayWithComplexTypes(rowMapping[++sourcePosition], complexQueryDims);
    }

    @Override
    public int getDimDataForAgg(int dimOrdinal)
    {
        return getSurrogateKey(rowMapping[currentRow], dimOrdinal);
    }

    @Override
    public byte[] getKeyArray()
    {
        ++currentRow;
        return getKeyArray(rowMapping[++sourcePosition],null);
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
                return mapOfDirectSurrogates.get(rowMapping[++sourcePosition]);
            }
            return mapOfDirectSurrogates.get(columnarKeyStoreMetadata.getColumnReverseIndex()[rowMapping[++sourcePosition]]);
        }
        return null;

    }
    
    @Override
    public void getComplexDimDataForAgg(GenericQueryType complexType, DataOutputStream dataOutputStream) throws IOException
    {
        getComplexSurrogateKey(rowMapping[currentRow], complexType, dataOutputStream);
    }
}
