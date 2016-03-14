/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBix1dJkG6yX4qXykVpzNHRfMYmCPFL+cpwFa1uDN7MPq/zssP/jY128wesqMq1XZX177
fzO4JVhhw7ChVW6gEzlFJgNf4NXLcMEuT6HxRHRXOyiL6Xpp0D68SNQOi32XDw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage;


/**
 * Member with name column. If ROLAP level is having name column, comparison
 * should happen on name column not on key column.
 * 
 * @author K00900207
 * 
 */
public class NameColumnMember
{
//    /**
//     * 
//     */
//    private int nameColumnIndex;
//
//    public NameColumnMember(byte[] name, int nameColIndex)
//    {
//        super(name);
//        this.nameColumnIndex = nameColIndex;
//    }
//
//    /**
//     * 
//     * @see com.huawei.unibi.molap.engine.datastorage.Member#equals(java.lang.Object)
//     */
//    public boolean equals(Object obj)
//    {
//        if(obj instanceof NameColumnMember)
//        {
//            if(this == obj)
//            {
//                return true;
//            }
//
//            return attributes[nameColumnIndex].equals(((NameColumnMember)obj).attributes[nameColumnIndex]);
//        }
//        else
//        {
//            if((obj instanceof Member))
//            {
//                return super.equals(obj);
//            } 
//            else 
//            {
//                return false;
//            }
//        }
//
//    }
//    
//    
//    /**
//     * 
//     * @see java.lang.Object#hashCode()
//     * 
//     */
//    @Override
//    public int hashCode()
//    {
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + attributes[nameColumnIndex].hashCode();
//        return result;
//    }
//
//    /*
//     * @Override public int compareTo(Member o) { char v1[] =
//     * ((String)attributes[nameColumnIndex]).toCharArray(); char v2[] =
//     * ((String)o.attributes[nameColumnIndex]).toCharArray();; int len1 =
//     * v1.length; int len2 = v2.length; int n = Math.min(len1, len2);
//     * 
//     * int k = 0; while (k <n) { char c1 = v1[k]; char c2 = v2[k]; if (c1 != c2)
//     * { return c1 - c2; } k++; }
//     * 
//     * return len1 - len2; }
//     */

}
