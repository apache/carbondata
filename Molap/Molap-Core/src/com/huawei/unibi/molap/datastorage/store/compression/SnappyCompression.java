/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;

/**
 * @author R00900208
 * 
 */
public class SnappyCompression
{
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(SnappyCompression.class.getName());

    /**
     * SnappyByteCompression.
     * @author S71955
     *
     */
    public static enum SnappyByteCompression implements Compressor<byte[]> {
        /**
         * 
         */
        INSTANCE;

        /**
         * wrapper method for compressing byte[] unCompInput.
         */
        public byte[] compress(byte[] unCompInput)
        {
            try
            {
                return Snappy.rawCompress(unCompInput, unCompInput.length);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
                return null;
            }
        }

        /**
         * wrapper method for unCompress byte[] compInput.
         * @param compInput.
         * @return byte[].
         */
        public byte[] unCompress(byte[] compInput)
        {
            try
            {
                return Snappy.uncompress(compInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
            }
            return compInput;
        }
    }

    /**
     * 
     * @author S71955
     * enum class for SnappyDoubleCompression.
     *
     */
    public static enum SnappyDoubleCompression implements Compressor<double[]> {
        /**
         * 
         */
        INSTANCE;

        /**
         * wrapper method for compressing double[] unCompInput.
         */
        public byte[] compress(double[] unCompInput)
        {
            try
            {
                return Snappy.compress(unCompInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
                return null;
            }
        }

        /**
         * wrapper method for unCompress byte[] compInput.
         * @param compInput.
         * @return byte[].
         */
        public double[] unCompress(byte[] compInput)
        {
            try
            {
                return Snappy.uncompressDoubleArray(compInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
            }
            return null;
        }

    }

    /**
     * enum class for SnappyShortCompression.
     * 
     * @author S71955
     * 
     */
    public static enum SnappyShortCompression implements Compressor<short[]> {
        /**
         * 
         */
        INSTANCE;


        /**
         * wrapper method for compress short[] unCompInput.
         * @param compInput.
         * @return byte[].
         */
        public byte[] compress(short[] unCompInput)
        {
            try
            {
                return Snappy.compress(unCompInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
                return null;
            }
        }

        /**
         * wrapper method for uncompressShortArray.
         * @param compInput.
         * @return byte[].
         */
        public short[] unCompress(byte[] compInput)
        {
            try
            {
                return Snappy.uncompressShortArray(compInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
            }
            return null;
        }
    }

    /**
     * 
     * @author S71955
     * enum class for SnappyIntCompression.
     *
     */
    public static enum SnappyIntCompression implements Compressor<int[]> {
        /**
         * 
         */
        INSTANCE;


        /**
         * wrapper method for compress int[] unCompInput.
         * @param compInput.
         * @return byte[].
         */
        public byte[] compress(int[] unCompInput)
        {
            try
            {
                return Snappy.compress(unCompInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
                return null;
            }
        }

        /**
         * wrapper method for uncompressIntArray.
         * @param compInput.
         * @return byte[].
         */
        public int[] unCompress(byte[] compInput)
        {
            try
            {
                return Snappy.uncompressIntArray(compInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
            }
            return null;
        }
    }

    /**
     * 
     * @author S71955
     * enum class for SnappyLongCompression.
     *
     */
    public static enum SnappyLongCompression implements Compressor<long[]> {
        /**
         * 
         */
        INSTANCE;


        /**
         * wrapper method for compress long[] unCompInput.
         * @param compInput.
         * @return byte[].
         */
        public byte[] compress(long[] unCompInput)
        {
            try
            {
                return Snappy.compress(unCompInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
                return null;
            }
        }

        /**
         * wrapper method for uncompressLongArray.
         * @param compInput.
         * @return byte[].
         */
        public long[] unCompress(byte[] compInput)
        {
            try
            {
                return Snappy.uncompressLongArray(compInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
            }
            return null;
        }
    }

    /**
     * 
     * @author S71955
     * enum class for SnappyFloatCompression.
     *
     */
    
    public static enum SnappyFloatCompression implements Compressor<float[]> {
        /**
         * 
         */
        INSTANCE;

        /**
         * wrapper method for compress float[] unCompInput.
         * @param compInput.
         * @return byte[].
         */
        public byte[] compress(float[] unCompInput)
        {
            try
            {
                return Snappy.compress(unCompInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
                return null;
            }
        }

        /**
         * wrapper method for uncompressFloatArray.
         * @param compInput.
         * @return byte[].
         */
        public float[] unCompress(byte[] compInput)
        {
            try
            {
                return Snappy.uncompressFloatArray(compInput);
            }
            catch(IOException e)
            {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
            }
            return null;
        }
    }

}
