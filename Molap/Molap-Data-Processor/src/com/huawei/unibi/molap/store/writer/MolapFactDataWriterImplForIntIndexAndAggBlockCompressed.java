package com.huawei.unibi.molap.store.writer;

import com.huawei.unibi.molap.datastorage.store.columnar.IndexStorage;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.keygenerator.mdkey.NumberCompressor;
import com.huawei.unibi.molap.util.MolapUtil;


public class MolapFactDataWriterImplForIntIndexAndAggBlockCompressed extends MolapFactDataWriterImplForIntIndexAndAggBlock
{
    
    private NumberCompressor[] keyBlockCompressor;
    
    public MolapFactDataWriterImplForIntIndexAndAggBlockCompressed(
            String storeLocation, int measureCount, int mdKeyLength,
            String tableName, boolean isNodeHolder,
            IFileManagerComposite fileManager, int[] keyBlockSize,
            boolean[] aggBlocks, int[] cardinality, boolean isUpdateFact)
    {
        super(storeLocation, measureCount, mdKeyLength, tableName, isNodeHolder,
                fileManager, keyBlockSize, aggBlocks,isUpdateFact);
        this.keyBlockCompressor= new NumberCompressor[cardinality.length];
        for(int i = 0;i < cardinality.length;i++)
        {
            this.keyBlockCompressor[i] = new NumberCompressor(
                    Long.toBinaryString(cardinality[i]).length(),
                    MolapUtil
                            .getIncrementedFullyFilledRCDCardinalityFullyFilled(cardinality[i]));
        }
    }
    
    //TODO SIMIAN
    protected byte[][] fillAndCompressedKeyBlockData(IndexStorage<int[]>[] keyStorageArray,int entryCount) 
    {
        byte[][] keyBlockData = new byte[keyStorageArray.length][];
        int destPos=0;
//        int normal=0;
        
        for(int i =0;i<keyStorageArray.length;i++)
        {
            destPos=0;
            if(aggBlocks[i])
            {
                keyBlockData[i]= new byte[keyStorageArray[i].getTotalSize()];
                for(int m=0;m<keyStorageArray[i].getKeyBlock().length;m++)
                {
                    System.arraycopy(keyStorageArray[i].getKeyBlock()[m], 0, keyBlockData[i], destPos, keyStorageArray[i].getKeyBlock()[m].length);
                    destPos+=keyStorageArray[i].getKeyBlock()[m].length;
                }
//                normal=keyBlockData[i].length;
                keyBlockData[i]= this.keyBlockCompressor[i].compressBytes(keyBlockData[i]);
//                System.out.println("Block Number: "+i + "Compressed Number Compressor: "+ (normal-keyBlockData[i].length));
            }
            else
            {
                keyBlockData[i]= new byte[entryCount* keyBlockSize[i]];
                for(int j=0;j<keyStorageArray[i].getKeyBlock().length;j++)
                {
                    System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i], destPos, keyBlockSize[i]);
                    destPos+=keyBlockSize[i];
                }
//                normal=keyBlockData[i].length;
                keyBlockData[i]= this.keyBlockCompressor[i].compressBytes(keyBlockData[i]);
//                System.out.println("Block Number: "+i + "Compressed Number Compressor: "+ (normal-keyBlockData[i].length));
//                normal=keyBlockData[i].length;
                keyBlockData[i]=SnappyByteCompression.INSTANCE.compress(keyBlockData[i]);
//                System.out.println("Block Number: "+i + "Compressed Snappy Compressor: "+ (normal-keyBlockData[i].length));
            }
           
        }
        return keyBlockData;
    }

}
