/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.compression;

import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

/**
 * @author r00900208
 * ValueCompressonHolder class.
 */
public final class ValueCompressonHolder
{

    /**
     * byteCompressor.
     */
    private static Compressor<byte[]> byteCompressor = SnappyCompression.SnappyByteCompression.INSTANCE;

    /**
     * shortCompressor.
     */
    private static Compressor<short[]> shortCompressor = SnappyCompression.SnappyShortCompression.INSTANCE;

    /**
     * intCompressor.
     */
    private static Compressor<int[]> intCompressor = SnappyCompression.SnappyIntCompression.INSTANCE;

    /**
     * longCompressor.
     */
    private static Compressor<long[]> longCompressor = SnappyCompression.SnappyLongCompression.INSTANCE;

    /**
     * floatCompressor
     */
    private static Compressor<float[]> floatCompressor = SnappyCompression.SnappyFloatCompression.INSTANCE;
   /**
    * doubleCompressor.
    */
    private static Compressor<double[]> doubleCompressor = SnappyCompression.SnappyDoubleCompression.INSTANCE;
    
    private ValueCompressonHolder()
    {
        
    }

    /**
     * interface for  UnCompressValue<T>.
     * @author S71955
     *
     * @param <T>
     */
    
    public interface UnCompressValue<T> extends Cloneable
    {
//        Object getValue(int index, int decimal, double maxValue);

        void setValue(T value);
        
        void setValueInBytes(byte[] value);

        UnCompressValue<T> getNew();

        UnCompressValue compress();

        UnCompressValue uncompress(DataType dataType);
        
        byte[] getBackArrayData();
        
        UnCompressValue getCompressorObject();
        
        MolapReadDataHolder getValues(int decimal, double maxValue);

    }

    /**
     * @param dataType
     * @param value
     * @param data
     */
    public static void unCompress(DataType dataType, UnCompressValue value, byte[] data)
    {
        switch(dataType)
        {
        case DATA_BYTE:
        
            value.setValue(byteCompressor.unCompress(data));
            break;
        
        case DATA_SHORT:
        
            value.setValue(shortCompressor.unCompress(data));
            break;
        
        case DATA_INT:
        
            value.setValue(intCompressor.unCompress(data));
            break;
        
        case DATA_LONG:
        
            value.setValue(longCompressor.unCompress(data));
            break;
        
        case DATA_FLOAT:
        
            value.setValue(floatCompressor.unCompress(data));
            break;
        default:
        
            value.setValue(doubleCompressor.unCompress(data));
            break;
        
        }
    }
    
}
