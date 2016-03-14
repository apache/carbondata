/**
 * 
 */
package com.huawei.unibi.molap.keygenerator;


/**
 * @author R00900208
 * 
 */
public class BitsByte
{
    
    /**
     * length.
     */
//    private int length;

    /**
     * lens.
     */
//    private int[] lens;

    /**
     * wsize.
     */
//    private int wsize;
    


    public BitsByte(int[] lens)
    {
//        this.lens = lens;
//        this.length = getTotalLength(lens);

//        wsize = length / 8;

//        if(length % 8 != 0)
//        {
//            wsize++;
//        }
    }

//    private int getTotalLength(int[] lens)
//    {
//        int tLen = 0;
//        for(int len : lens)
//        {
//            tLen += len;
//        }
//        return tLen;
//    }




    // public byte[] getBytes(Long[] keys)
    // {
    //
    // long[] words = get(keys);
    //
    // int length = 8;
    // byte[] bytes = new byte[length * words.length];
    //
    // for (int i = 0; i < words.length; i++)
    // {
    // long val = words[i];
    //
    // for (int j = length-1; j > 0; j--)
    // {
    // bytes[i * length + j] = (byte) val;
    // val >>>= 8;
    // }
    //
    // bytes[i * length] = (byte) val;
    // }
    // return bytes;
    //
    // }

  

    /*public static void main(String[] args)
    {
        BitsByte bits = new BitsByte(new int[]{5, 6, 5});

        byte[] b = bits.get(new Long[]{4L, 5L, 6L});
        bits.getKeyArray(b);
        // bits.getKeyArray(b);
    }*/

}
