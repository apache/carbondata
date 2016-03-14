package com.huawei.unibi.molap.engine.evaluators.conditional.dimcolumns;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.evaluators.FilterProcessorPlaceHolder;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.util.MolapUtil;

public class UniqueBlockEqualsEvalutor extends NonUniqueBlockEqualsEvalutor
{
    public UniqueBlockEqualsEvalutor(Expression exp, boolean isExpressionResolve, boolean isIncludeFilter)
    {
        super(exp,isExpressionResolve,isIncludeFilter);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder dataBlockHolder, FilterProcessorPlaceHolder placeHolder)
    {
        if(null == dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()])
        {
            dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()] = dataBlockHolder
                    .getLeafDataBlock().getColumnarKeyStore(dataBlockHolder.getFileHolder(),
                            dimColEvaluatorInfoList.get(0).getColumnIndex(),
                            dimColEvaluatorInfoList.get(0).isNeedCompressedData());
        }
        
        if(dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()]
                .getColumnarKeyStoreMetadata().isUnCompressed())
        {
            return super.applyFilter(dataBlockHolder, placeHolder);
        }
        return getFilteredIndexes(
                dataBlockHolder.getColumnarKeyStore()[dimColEvaluatorInfoList.get(0).getColumnIndex()], dataBlockHolder
                        .getLeafDataBlock().getnKeys());
    }

    private BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows)
    {
        int[] colIndex = keyBlockArray.getColumnarKeyStoreMetadata().getColumnIndex();
        int[] dataIndex = keyBlockArray.getColumnarKeyStoreMetadata().getDataIndex();
        int startIndex = 0;
        int lastIndex = dataIndex.length == 0 ? numerOfRows - 1 : dataIndex.length / 2 - 1;
        BitSet bitSet = new BitSet(numerOfRows);
        for(int i = 0;i < dimColEvaluatorInfoList.get(0).getFilterValues().length;i++)
        {
            int index = MolapUtil.getIndexUsingBinarySearch(keyBlockArray, startIndex, lastIndex,
                    dimColEvaluatorInfoList.get(0).getFilterValues()[i]);
            if(index == -1)
            {
                continue;
            }
            if(dataIndex.length ==0)
            {
                if(null != colIndex)
                {
                    bitSet.set(colIndex[index]);
                }
                else
                {
                    bitSet.set(index);
                }
                continue;
            }

            startIndex = index + 1;
            int last = dataIndex[index * 2] + dataIndex[index * 2 + 1];
            if(null != colIndex)
            {
                for(int start = dataIndex[index * 2];start < last;start++)
                {
                    bitSet.set(colIndex[start]);
                }
            }
            else
            {
                for(int start = dataIndex[index * 2];start < last;start++)
                {
                    bitSet.set(start);
                }
            }
        }
        return bitSet;
    }

}
