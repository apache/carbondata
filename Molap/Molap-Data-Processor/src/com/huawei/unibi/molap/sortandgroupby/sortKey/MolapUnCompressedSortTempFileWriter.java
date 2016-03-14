package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapUtil;

public class MolapUnCompressedSortTempFileWriter extends AbstractMolapSortTempFileWriter
{

    /**
     * MolapCompressedSortTempFileWriter
     * @param measureCount
     * @param mdkeyIndex
     * @param mdKeyLength
     * @param isFactMdkeyInSort
     * @param factMdkeyLength
     * @param writeFileBufferSize
     */
    public MolapUnCompressedSortTempFileWriter(int measureCount, int mdkeyIndex, int mdKeyLength,
            boolean isFactMdkeyInSort, int factMdkeyLength,
            int writeFileBufferSize, char[] type)
    {
        super(measureCount, mdkeyIndex, mdKeyLength, isFactMdkeyInSort, factMdkeyLength, writeFileBufferSize,type);
    }

    /**
     * Below method will be used to write the sort temp file
     * @param records
     */
    public void writeSortTempFile(Object[][] records)
            throws MolapSortKeyAndGroupByException
    {
        ByteArrayOutputStream blockDataArray = null;
        DataOutputStream dataOutputStream = null;
        Object[] row= null;
        int totalSize=0;
        try
        {
            totalSize=records.length
                    * MolapCommonConstants.INT_SIZE_IN_BYTE+this.mdKeyLength*records.length;
            if(isFactMdkeyInSort)
            {
                totalSize+=this.factMdkeyLength*records.length;
            }
            blockDataArray = new ByteArrayOutputStream(totalSize);
            dataOutputStream = new DataOutputStream(blockDataArray);
            for(int i = 0;i < records.length;i++)
            {
                row = records[i];
                for(int j = 0;j < measureCount;j++)
                {
                    if(type[j]!='c')
                    {
                        if(null != row[j])
                        {
                            dataOutputStream.write((byte)1);
                            dataOutputStream.writeDouble((Double)row[j]);
                        }
                        else
                        {
                            dataOutputStream.write((byte)0);
                        }
                    }
                    else
                    {
                        dataOutputStream.writeInt(((byte[])row[j]).length);
                        dataOutputStream.write((byte[])row[j]);
                    }
                }
                dataOutputStream.write((byte[])row[mdkeyIndex]);
                if(isFactMdkeyInSort)
                {
                    dataOutputStream.write((byte[])row[row.length - 1]);
                }
            }
            stream.writeInt(records.length);
            byte[] byteArray = blockDataArray.toByteArray();
            stream.writeInt(byteArray.length);
            stream.write(byteArray);

        }
        catch(IOException e)
        {
            throw new MolapSortKeyAndGroupByException(e);
        }
        finally
        {
            MolapUtil.closeStreams(blockDataArray);
            MolapUtil.closeStreams(dataOutputStream);
        }
    }
}
