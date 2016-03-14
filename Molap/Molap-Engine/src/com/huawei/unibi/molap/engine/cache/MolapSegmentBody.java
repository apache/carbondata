/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBr/sotoVRstWv2DiJXiystf8iWoMq4MBXVHIt88B3iQ5vBSVBbCfB8wDrR44TAK/MmZd
pXCVqXa8ocTtqjgKCQwnHts096RbzXwwl6UNO5Euc9TLBdNoX/kk2lPsBJuA3Q==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.cache;

import java.io.Serializable;

/**
 * @author R00900208
 *
 */
public class MolapSegmentBody implements Serializable
{
//
//    /**
//     * 
//     */
//    private static final long serialVersionUID = -2326770749402262945L;
//
//    private Serializable data;
//    
//    private Object measures;
//    
//    private Object calcMsrs;
//    
//    /**
//     * maskedKeyRanges
//     */
//    private int[] maskedKeyRanges;
//    
//    /**
//     * maxKey
//     */
//    private byte[] maxKey;
//    
//    private int keySize;
//    
//    private String[] aggNames;
//    
//    /**
//     * sortOrder
//     */
//    private byte[] sortOrder;
//    
//    private KeyGenerator generator;
//    
//    private int size;
//    
//
//
//    public MolapSegmentBody(Serializable data, String[] measures)
//    {
//        this.data = data;
//        this.measures = measures;
//    }
//
//    /**
//     * @return the data
//     */
//    public Serializable getData()
//    {
//        return data;
//    }
//
//    /**
//     * @return the measures
//     */
//    public String[] getMeasures()
//    {
//        return (String[])measures;
//    }
//
//    /**
//     * Merges the data with another segment body
//     * @param toMolapSegmentBody
//     */
//    public void merge(MolapSegmentBody toMolapSegmentBody)
//    {
//      String[] toMergeMeasures = toMolapSegmentBody.getMeasures();
//      String[] msrs = (String[])measures;
//      
//      HashMap<ByteArrayWrapper, MeasureAggregator[]> toMergeData =  ((HashMap<ByteArrayWrapper, MeasureAggregator[]>) toMolapSegmentBody.getData());
//      HashMap<ByteArrayWrapper, MeasureAggregator[]> originalMergeData =  ((HashMap<ByteArrayWrapper, MeasureAggregator[]>) data);
//      
//      HashSet<String> setr = new HashSet<String>();
//      setr.addAll(Arrays.asList(msrs));
//      setr.addAll(Arrays.asList(toMergeMeasures));
//      
//      String[] targetMeasures = new String[setr.size()];
//      BitKey bitMap = formMeasureList(msrs,toMergeMeasures,setr.size());
//    
//      for(Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = originalMergeData.entrySet().iterator();iterator.hasNext();)
//      {
//        MeasureAggregator[] target = new MeasureAggregator[setr.size()];
//
//        Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
//         
//        ByteArrayWrapper keyWrapper = entry.getKey();
//        MeasureAggregator[] originalMeasureAggregators = entry.getValue();
//        MeasureAggregator[] toMergeMeasureAggregators = toMergeData.get(keyWrapper);
//
//        int j = 0;
//        int k = 0;
//        for(int i=0;i<setr.size();i++)
//        {
//            if(bitMap.get(i))
//            {//CHECKSTYLE:OFF    Approval No:Approval-331,332
//                targetMeasures[i] = msrs[j];
//                target[i]=originalMeasureAggregators[j];
//                j++;
//            }//CHECKSTYLE:ON
//            else
//            {//CHECKSTYLE:OFF    Approval No:Approval-333,334
//                targetMeasures[i] = toMergeMeasures[k];
//                target[i]=toMergeMeasureAggregators[k];
//                k++;
//            }//CHECKSTYLE:ON
//        }
//        ((HashMap<ByteArrayWrapper, MeasureAggregator[]>) data).put(keyWrapper, target);
//      }
//      
//      String[] toMergeAggNames = toMolapSegmentBody.getAggNames();
//      
//      aggNames = mergeAggNames(targetMeasures, toMergeAggNames, toMergeMeasures, getMeasures(), aggNames);     
//      measures = targetMeasures;
//    }
//    
//    private String[] mergeAggNames(String[] targetMeasures,String[] toMergeAggNames,String[] toMergeMeasures,String[] msrs,String[] aggNames)
//    {
//        String[] targetAggNames = new String[targetMeasures.length];
//        for(int i = 0;i < targetMeasures.length;i++)
//        {
//            boolean found = false;
//          //CHECKSTYLE:OFF    Approval No:Approval-368
//            for(int j = 0;j < msrs.length;j++)
//            {
//                if(targetMeasures[i].equals(msrs[j]))
//                {
//                    targetAggNames[i] = aggNames[j];
//                    found = true;
//                    break;
//                }
//            }
//            if(!found)
//            {
//                for(int j = 0;j < toMergeMeasures.length;j++)
//                {
//                    if(targetMeasures[i].equals(toMergeMeasures[j]))
//                    {
//                        targetAggNames[i] = toMergeAggNames[j];
//                        break;
//                    }
//                }
//              //CHECKSTYLE:ON
//            }
//            
//        }
//        return targetAggNames;
//        
//    }
//
//    /**
//     * Lists which to pick from latest segment and which from old segment
//     * @param msrs
//     * @param toMergeMeasures
//     * @param sizeOfResultant
//     * @return
//     */
//    private BitKey formMeasureList(String[] msrs, String[] toMergeMeasures, int sizeOfResultant)
//    {
//        BitKey bitMap;
//        bitMap = BitKey.Factory.makeBitKey(sizeOfResultant);
//        for(int i=0;i<msrs.length;i++)
//        {
//            for(int j=0;j<toMergeMeasures.length;j++)
//            {
//                if(msrs[i].equals(toMergeMeasures[j]))
//                {
//                    bitMap.set(i);
//                }
//            }
//            if(!bitMap.get(i))
//            {
//                bitMap.set(i);
//            }
//        }
//        return bitMap;
//        
//    }
//
//
//
//    /**
//     * trims the data to be < startkey specified
//     * @param startKeySurrogate
//     */
//    public void trim(byte[] startKeySurrogate)
//    {
//        byte[] maskedKey = new byte[maskedKeyRanges.length];
//        int counter = 0;
//        int byteRange = 0;
//        for(int i = 0;i < maskedKeyRanges.length;i++)
//        {
//            byteRange = maskedKeyRanges[i];
//            maskedKey[counter++] = (byte)(startKeySurrogate[byteRange] & maxKey[byteRange]);
//        }
//        
//        HashMap<ByteArrayWrapper, MeasureAggregator[]> originalData = ((HashMap<ByteArrayWrapper, MeasureAggregator[]>)data);
//        for(Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = originalData.entrySet().iterator();iterator
//                .hasNext();)
//        {
//            Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
//            ByteArrayWrapper keyWrapper = entry.getKey();
//            if(ByteUtil.compare(keyWrapper.getMaskedKey(), maskedKey) >= 0)
//            {
//                iterator.remove();
//            }
//        }
//    }
//    
//    /**
//     * It makes the copy of this object.
//     * @return
//     */
//    public MolapSegmentBody getCopy()
//    {
//        Map<ByteArrayWrapper, MeasureAggregator[]> createDataCopy = createDataCopy((Map<ByteArrayWrapper, MeasureAggregator[]>)data);
//        MolapSegmentBody body = new MolapSegmentBody((Serializable)createDataCopy, (String[])measures);
//        body.calcMsrs = calcMsrs;
//        body.maskedKeyRanges = maskedKeyRanges;
//        body.maxKey = maxKey;
//        body.keySize = keySize;
//        body.aggNames = aggNames;
//        body.sortOrder = sortOrder;
//        body.size = size;
//        body.generator = generator;
//        return body;
//    }
//    
//    
//    private Map<ByteArrayWrapper, MeasureAggregator[]> createDataCopy(Map<ByteArrayWrapper, MeasureAggregator[]> data)
//    {
//        if(data == null)
//        {
//            return null;
//        }
//        Map<ByteArrayWrapper, MeasureAggregator[]> copy = new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>();
//        
//        for(Entry<ByteArrayWrapper, MeasureAggregator[]> entry : data.entrySet())
//        {
//            ByteArrayWrapper copyWrapper = new ByteArrayWrapper();
//            copyWrapper.setMaskedKey(entry.getKey().getMaskedKey());
//            MeasureAggregator[] copyAgg = new MeasureAggregator[entry.getValue().length];
//            for(int i = 0;i < copyAgg.length;i++)
//            {
//                copyAgg[i] = entry.getValue()[i].getCopy();
//            }
//            copy.put(copyWrapper, copyAgg);
//        }
//        
//        return copy;
//    }
//    
//
//    /**
//     * @return the maskedKeyRanges
//     */
//    public int[] getMaskedKeyRanges()
//    {
//        return maskedKeyRanges;
//    }
//
//    /**
//     * @param maskedKeyRanges the maskedKeyRanges to set
//     */
//    public void setMaskedKeyRanges(int[] maskedKeyRanges)
//    {
//        this.maskedKeyRanges = maskedKeyRanges;
//    }
//
//    /**
//     * @return the maxKey
//     */
//    public byte[] getMaxKey()
//    {
//        return maxKey;
//    }
//
//    /**
//     * @param maxKey the maxKey to set
//     */
//    public void setMaxKey(byte[] maxKey)
//    {
//        this.maxKey = maxKey;
//    }
//
//    /**
//     * @return the calcMsrs
//     */
//    public String[] getCalcMsrs()
//    {
//        return (String[])calcMsrs;
//    }
//
//    /**
//     * @param calcMsrs the calcMsrs to set
//     */
//    public void setCalcMsrs(Object calcMsrs)
//    {
//        this.calcMsrs = calcMsrs;
//    }
//
//
//    /**
//     * @return the keySize
//     */
//    public int getKeySize()
//    {
//        return keySize;
//    }
//
//    /**
//     * @param keySize the keySize to set
//     */
//    public void setKeySize(int keySize)
//    {
//        this.keySize = keySize;
//    }
//
//    /**
//     * @param data the data to set
//     */
//    public void setData(Serializable data)
//    {
//        this.data = data;
//    }
//
//    /**
//     * @return the aggNames
//     */
//    public String[] getAggNames()
//    {
//        return aggNames;
//    }
//
//    /**
//     * @param aggNames the aggNames to set
//     */
//    public void setAggNames(String[] aggNames)
//    {
//        this.aggNames = aggNames;
//    }
//
//    /**
//     * @return the sortOrder
//     */
//    public byte[] getSortOrder()
//    {
//        return sortOrder;
//    }
//
//    /**
//     * @param sortOrder the sortOrder to set
//     */
//    public void setSortOrder(byte[] sortOrder)
//    {
//        this.sortOrder = sortOrder;
//    }
//    
//    /**
//     * @return the size
//     */
//    public int getSize()
//    {
//        return size;
//    }
//
//    /**
//     * @param size the size to set
//     */
//    public void setSize(int size)
//    {
//        this.size = size;
//    }
//
//    /**
//     * @return the generator
//     */
//    public KeyGenerator getGenerator()
//    {
//        return generator;
//    }
//
//    /**
//     * @param generator the generator to set
//     */
//    public void setGenerator(KeyGenerator generator)
//    {
//        this.generator = generator;
//    }
}
