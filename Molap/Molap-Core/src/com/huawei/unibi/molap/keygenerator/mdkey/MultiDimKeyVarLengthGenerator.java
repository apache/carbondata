/**
 * 
 */
package com.huawei.unibi.molap.keygenerator.mdkey;

//import com.huawei.iweb.platform.logging.LogService;
//import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.keygenerator.KeyGenException;

/**
 * @author R00900208
 * 
 */
public class MultiDimKeyVarLengthGenerator extends AbstractKeyGenerator
{


    /**
     * serialVersionUID.
     */
    private static final long serialVersionUID = 9134778127271586515L;
    

    /**
     * bits.
     */
    private Bits bits;
    
    private int startAndEndKeySizeWithPrimitives;

    /**
     * 
     */
    protected int[][] byteRangesForKeys;
    
    /**
     * Attribute for Molap LOGGER.
     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(MultiDimKeyVarLengthGenerator.class.getName());

    public MultiDimKeyVarLengthGenerator(int[] lens)
    {
        bits = new Bits(lens);
        byteRangesForKeys = new int[lens.length][];
        int keys = lens.length;
        for(int i = 0;i < keys;i++)
        {
            byteRangesForKeys[i] = bits.getKeyByteOffsets(i);
        }
    }

    @Override
    public byte[] generateKey(long[] keys) throws KeyGenException
    {

        return bits.getBytes(keys);
    }
    
    @Override
    public byte[] generateKey(int[] keys) throws KeyGenException
    {

        return bits.getBytes(keys);
    }

    @Override
    public long[] getKeyArray(byte[] key)
    {

        return bits.getKeyArray(key);
    }

    @Override
    public long getKey(byte[] key, int index)
    {

        return bits.getKeyArray(key)[index];
    }

    /**
     * 
     * @see com.huawei.unibi.molap.keygenerator.mdkey.AbstractKeyGenerator#
     *      getKeySizeInBytes()
     */
    public int getKeySizeInBytes()
    {
        return bits.getByteSize();
    }

    @Override
    public long[] getSubKeyArray(byte[] key, int index, int size)
    {
        if(index < 0 || size == 0)
        {
            return null;
        }
        long[] keys = bits.getKeyArray(key);
        long[] rtn = new long[size];
//        for(int i = 0;i < size;i++)
//        {
//            rtn[i] = keys[i + index];
//        }
        System.arraycopy(keys, index, rtn, 0, size);
        return rtn;
    }

    @Override
    public int[] getKeyByteOffsets(int index)
    {
        return byteRangesForKeys[index];
    }

    @Override
    public int getDimCount()
    {

        return bits.getDimCount();
    }
    
    
    /**
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     * 
     */
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof MultiDimKeyVarLengthGenerator)
        {
            MultiDimKeyVarLengthGenerator other = (MultiDimKeyVarLengthGenerator)obj;
            return bits.equals(other.bits);
        }
        
        return false;
    }
    
    @Override
    public int hashCode()
    {
        return bits.hashCode();
    }

    @Override
    public long[] getKeyArray(byte[] key, int[] maskedByteRanges)
    {
        return bits.getKeyArray(key, maskedByteRanges);
    }

    @Override
    public void setStartAndEndKeySizeWithOnlyPrimitives(int startAndEndKeySizeWithPrimitives)
    {
        this.startAndEndKeySizeWithPrimitives = startAndEndKeySizeWithPrimitives;
    }

    @Override
    public int getStartAndEndKeySizeWithOnlyPrimitives()
    {
        return startAndEndKeySizeWithPrimitives;
    }

    /**
     * @param args
     * @throws MalformedURLException
     */
    /*public static void main(String[] args) throws MalformedURLException
    {
        // int i= 0 ;
        // if(i==0)
        // test1();
        // else
        // test2();
        // String path = "F:/TRP Demo/jars/skeys/skeys_0";
        //
        // for(int i=0; i<41; i++)
        // {
        // LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, path+ i + ".txt");
        // test(path+i+".txt");
        // }

        // test("F:/TRP Demo/jars/skeys_0_sorted.txt");

        testsw();

    }*/

   /* private static void test2()
    {
        long[] keymax = new long[4];
        Arrays.fill(keymax, 0l);
        keymax[0] = Long.MAX_VALUE;
        try
        {
            MultiDimKeyVarLengthGenerator keyGen = new MultiDimKeyVarLengthGenerator(new int[]{16, 16, 16, 16});
            byte[] maxKey = keyGen.generateKey(keymax);

            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.asList(keymax));
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.asList(keyGen.getKeyArray(maxKey)));

            long keyLong[] = new long[]{65500, 3, 3, 88};
            byte[] key = keyGen.generateKey(keyLong);

            byte[] temp = new byte[key.length];
            for(int j = 0;j < key.length;j++)
            {
                temp[j] = (byte)(key[j] & maxKey[j]);
            }
            long[] maskedKey = keyGen.getKeyArray(temp);
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.asList(maskedKey));
        }
        catch(KeyGenException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
    }*/

    /*private static void testsw()
    {

        long[] keymax = new long[12];
        Arrays.fill(keymax, Long.MAX_VALUE);
        try
        {
            MultiDimKeyVarLengthGenerator keyGen = new MultiDimKeyVarLengthGenerator(new int[]{3, 7, 7, 7, 7, 7, 7, 7,
                    8, 8, 8, 4});

            byte[] maxKey = keyGen.generateKey(keymax);

            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.asList(keymax));
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.toString(maxKey));
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.toString(keyGen.getKeyArray(maxKey)));

            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, keyGen.getKeySizeInBytes());
        }
        catch(KeyGenException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }

    }*/

    /*private static void test1()
    {
        long[] keymax = new long[12];
        Arrays.fill(keymax, 0l);
        keymax[4] = Long.MAX_VALUE;
        try
        {
            MultiDimKeyVarLengthGenerator keyGen = new MultiDimKeyVarLengthGenerator(new int[]{3, 7, 7, 7, 7, 7, 7, 7,
                    9, 9, 9, 4});
            byte[] maxKey = keyGen.generateKey(keymax);

            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.asList(keymax));
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.asList(keyGen.getKeyArray(maxKey)));

            long keyLong[] = new long[]{3, 3, 42, 88, 111, 2, 1, 43, 2, 3, 7, 1};
            byte[] key = keyGen.generateKey(keyLong);

            byte[] temp = new byte[key.length];
            for(int j = 0;j < key.length;j++)
            {
                temp[j] = (byte)(key[j] & maxKey[j]);
            }

            keyGen.getKeyArray(temp);
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.asList(keyGen.getKeyArray(temp)));
        }
        catch(KeyGenException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
    }*/

    /*private static void test(String path)
    {
        int[] lens = new int[]{5, 5, 5, 4, 6, 4, 7, 6, 4, 7, 14, 20, 12};
        long[] maxs = new long[]{1L, 1L, 12L, 4L, 35L, 8L, 56L, 31L, 5L, 100L, 10000L, 999999L, 2558l};

        int counter = 0;
        BufferedReader reader = null;
        try
        {
            MultiDimKeyVarLengthGenerator keyGen = new MultiDimKeyVarLengthGenerator(lens);

            reader = new BufferedReader(new FileReader(path));
            String line = null;
            String[] oneLine = null;
            long aKey[] = new long[13];
            long genKey[] = null;
            byte mdKey[] = null;
            while((line = reader.readLine()) != null)
            {
                // LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, line);
                if(line.trim().isEmpty())
                    continue;
                counter++;
                aKey = new long[13];
                oneLine = line.split(",");
                for(int i = 0;i < 13;i++)
                {
                    aKey[i] = Long.parseLong(oneLine[i].trim());
                }

                mdKey = keyGen.generateKey(aKey);
                genKey = keyGen.getKeyArray(mdKey);
                if(!Arrays.equals(genKey, aKey))
                {
                    LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, counter);
                    LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.toString(aKey));
                    LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.toString(genKey));
                    break;
                }

                for(int p = 0;p < 13;p++)
                {
                    if(genKey[p] == 0 || genKey[p] > maxs[p])
                    {
                        LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, line);
                        LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.toString(genKey));
                        return;
                    }
                }
            }

            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "No problem");

        }
        catch(Exception e)
        {
            // TODO Auto-generated catch block
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, counter);
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        finally
        {
            if(reader != null)
            {
                try
                {
                    reader.close();
                }
                catch(IOException e)
                {
                    LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
                }
            }
        }

    }*/
}
