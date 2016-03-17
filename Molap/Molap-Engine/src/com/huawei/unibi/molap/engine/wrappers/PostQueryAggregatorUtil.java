/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7DdiJbBTq8EhkB53TfdcDK50O3qlO5u854V6/HKb13m05YsaRHsc/nthRMbx8Ii1nyuh
EypS8h+bwtoINUUGEglUtLfxNJpOeAJMaPNusmJj8XBfZW6Bqsxpo8UEx1QuZQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.wrappers;

import java.util.Arrays;



/**
 * It aggregates the data from the already existed data.
 * 
 * @author R00900208
 *
 */
public class PostQueryAggregatorUtil
{

//    /**
//     * 
//     * @param data
//     * @param keyGenerator
//     * @param actualDims
//     * @param requiredDims
//     * @param actualMeasures
//     * @param requiredMeasure
//     */
//    public static Map<ByteArrayWrapper, MeasureAggregator[]> aggregateData(MolapSegmentBody segmentBodyFromCache, KeyGenerator keyGenerator,
//            List<Measure> msrs, byte[] maskedBytesForRollUp,int[] bytePosArray)
//    {
//        Map<ByteArrayWrapper, MeasureAggregator[]> checkMatchAndReturnData = checkMatchAndReturnData(segmentBodyFromCache, msrs);
//        Map<ByteArrayWrapper, MeasureAggregator[]> aggData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>();
//        ByteArrayWrapperExtended extended = new ByteArrayWrapperExtended();
//        /**
//         * Fortify Fix: NULL_RETURNS
//         */
//        if(null != checkMatchAndReturnData)
//        {
//            for(Entry<ByteArrayWrapper, MeasureAggregator[]> entry : checkMatchAndReturnData.entrySet())
//            {
//                extended.setData(entry.getKey().getMaskedKey(),bytePosArray,maskedBytesForRollUp);
//                MeasureAggregator[] value = entry.getValue();
//                MeasureAggregator[] measureAggregators = aggData.get(extended);
//                if(measureAggregators != null)
//                {
//                    for(int i = 0;i < measureAggregators.length;i++)
//                    {
//                        measureAggregators[i].merge(value[i]);
//                    }
//                }
//                else
//                {
//                    aggData.put(extended, value);
//                    extended = new ByteArrayWrapperExtended();
//                }
//            }
//        }
//        return aggData;
//    }
//    
//    /**
//     * 
//     * @param data
//     * @param keyGenerator
//     * @param actualDims
//     * @param requiredDims
//     * @param actualMeasures
//     * @param requiredMeasure
//     * @throws KeyGenException 
//     */
//    public static Map<ByteArrayWrapper, MeasureAggregator[]> aggregateData(MolapSegmentBody segmentBodyFromCache, KeyGenerator keyGenerator,
//            List<Measure> msrs, byte[] maskedBytesForRollUp,int[] bytePosArray,InMemFilterModel filterModel,int[] maskedBytePos,Dimension[] dimensions,List<InMemoryCube> slices) throws KeyGenException
//    {
//        Map<ByteArrayWrapper, MeasureAggregator[]> checkMatchAndReturnData = checkMatchAndReturnData(segmentBodyFromCache, msrs);
//        Map<ByteArrayWrapper, MeasureAggregator[]> aggData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>();
//        
//        KeyFilterImpl filterImpl = new KeyFilterImpl(filterModel, keyGenerator, new long[keyGenerator.getDimCount()]);
//        
//        ByteArrayWrapperExtended extended = new ByteArrayWrapperExtended();
//        for(Entry<ByteArrayWrapper, MeasureAggregator[]> entry : checkMatchAndReturnData.entrySet())
//        {
//            byte[] maskedKey = entry.getKey().getMaskedKey();
//            if(filterImpl.filterKey(getByteArray(maskedBytePos,maskedKey,keyGenerator,dimensions,slices)))
//            {
//                extended.setData(maskedKey,bytePosArray,maskedBytesForRollUp);
//                MeasureAggregator[] value = entry.getValue();
//                MeasureAggregator[] measureAggregators = aggData.get(extended);
//                if(measureAggregators != null)
//                {
//                    for(int i = 0;i < measureAggregators.length;i++)
//                    {
//                        measureAggregators[i].merge(value[i]);
//                    }
//                }
//                else
//                {
//                    aggData.put(extended, value);
//                    extended = new ByteArrayWrapperExtended();
//                }
//            }
//        }
//        return aggData;
//    }
//    
//    
//    private static byte[] getByteArray(int[] maskedBytePos,byte[] maskKey,KeyGenerator generator,Dimension[] dimensions,List<InMemoryCube> slices) throws KeyGenException
//    {
////        byte[] actual = new byte[maskedBytePos.length];
////        for(int i = 0;i < actual.length;i++)
////        {
////            if(maskedBytePos[i] != -1)
////            {
////                actual[i] = maskKey[maskedBytePos[i]];
////            }
////        }
////        return actual;
//        
//        long[] keyArray = generator.getKeyArray(maskKey, maskedBytePos);
//        for(int i = 0;i < dimensions.length;i++)
//        {
//          //CHECKSTYLE:OFF    Approval No:Approval-358
//            keyArray[dimensions[i].getOrdinal()] = getActualSurrogateKeyFromSortedIndex(dimensions[i],
//                    (int)keyArray[dimensions[i].getOrdinal()],slices);
//          //CHECKSTYLE:ON
//        }
//        return generator.generateKey(keyArray);
//        
//    }
//    
//    
//    /**
//     * Below method will be used to get the sor index 
//     * @param columnName
//     * @param id
//     * @return sort index 
//     */
//    private static int getActualSurrogateKeyFromSortedIndex(Dimension columnName, int id,List<InMemoryCube> slices)
//    {
//        for(InMemoryCube slice : slices)
//        {
//            int index = slice.getMemberCache(columnName.getTableName()+'_'+columnName.getColName()+ '_' + columnName.getDimName() + '_' + columnName.getHierName()).getActualSurrogateKeyFromSortedIndex(id);
//            if(index != -MolapCommonConstants.DIMENSION_DEFAULT)
//            {
//                return index;
//            }
//        }
//        return -MolapCommonConstants.DIMENSION_DEFAULT;
//    }
  //CHECKSTYLE:OFF    Approval No:Approval-368

    /**
     * ByteArrayWrapperExtended
     * @author R00900208
     *
     */
    public static class ByteArrayWrapperExtended extends ByteArrayWrapper
    {
        /**
         * 
         */
        private static final long serialVersionUID = 5575009899945725708L;
        private  byte[] maskMaskKey;
        
        public ByteArrayWrapperExtended()
        {
        }
        
        /**
         * Set the data
         * 
         * @param maskedKey
         * @param bytePosArray
         * @param maskedBytesForRollUp
         *
         */
        public void setData(byte[] maskedKey, int[] bytePosArray,byte[] maskedBytesForRollUp)
        {
//            this.maskedKey = maskedKey;
            if(maskMaskKey == null)
            {
                maskMaskKey = new byte[bytePosArray.length];
            }
            for(int i = 0;i < bytePosArray.length;i++)
            {
//                int pos = bytePosArray[i];
                maskMaskKey[i] = (byte)(maskedBytesForRollUp[i]&maskedKey[bytePosArray[i]]);
            }
            this.maskedKey = maskMaskKey;
        }
        
        @Override
        public boolean equals(Object other)
        {
            // check whether other is type of ByteArrayWrapper
            if(other instanceof ByteArrayWrapperExtended)
            {
                return Arrays.equals(maskMaskKey, ((ByteArrayWrapperExtended)other).maskMaskKey);
            }
            if(other instanceof ByteArrayWrapper)
            {
                return super.equals(other);
            }
            return false;
        }
        
        @Override
        public int hashCode()
        {
            int len = maskMaskKey.length;
            int result = 1;
            for(int j = 0;j < len;j++)
            {
                result = 31 * result + maskMaskKey[j];
            }
            return result;
        }
        
    }
  //CHECKSTYLE:ON

    
    
//    /**
//     * Returns the segmentBody data if segment is the superset of msrs
//     * @param segmentBodyFromCache
//     * @param msrs
//     * @return
//     */
//    public static Map<ByteArrayWrapper, MeasureAggregator[]> checkMatchAndReturnData(MolapSegmentBody segmentBodyFromCache, List<Measure> msrs)
//    {
//        String[] segmentBodyMeasures = segmentBodyFromCache.getMeasures();
//        Measure[] measures = msrs.toArray(new Measure[msrs.size()]);
//        String[] measuresName = new String[msrs.size()];
//        int i=0;
//        for(Measure measure : measures)
//        {
//            measuresName[i++] = measure.getName();
//        }
//        
//        List<String> measuresNameList = Arrays.asList(measuresName);
//        List<String> segmentBodyMeasuresList = Arrays.asList(segmentBodyMeasures);
//        if(segmentBodyMeasuresList.equals(measuresNameList))
//        {
//            return  (Map<ByteArrayWrapper, MeasureAggregator[]>)segmentBodyFromCache.getData();
//        }
//        if(segmentBodyMeasuresList.containsAll(measuresNameList))
//        {
//            boolean[] bs = new boolean[msrs.size()];
//            Arrays.fill(bs, true);
//            Map<ByteArrayWrapper, MeasureAggregator[]> dataForMsrs = PostQueryAggregatorUtil.getDataForMsrs(msrs,
//                    bs, segmentBodyFromCache.getMeasures(),
//                    (Map<ByteArrayWrapper, MeasureAggregator[]>)segmentBodyFromCache.getData());
//            boolean allNull = false;
//            for(int j = 0;j < bs.length;j++)
//            {
//                if(bs[j])
//                {
//                    allNull = true;
//                }
//            }
//            if(allNull)
//            {
//                return null;
//            }
//            return dataForMsrs;
//        }
//        else
//        {
//            return null;
//        }
//    }
//    
//    /**
//     * Fetches data for only specified measure(returns subset)
//     * @param msrsRequested
//     * @return
//     */
//    public static Map<ByteArrayWrapper, MeasureAggregator[]> getDataForMsrs(List<Measure> msrsRequested,boolean[] isNullMeasure,String[] msrs,Map<ByteArrayWrapper, MeasureAggregator[]> originalData)
//    {
//        Map<ByteArrayWrapper, MeasureAggregator[]> toReturn =  new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>();
//        Measure[] msrsRequestedArray = msrsRequested.toArray(new Measure[msrsRequested.size()]);
//        String[] msrsRequestedNames = new String[msrsRequestedArray.length];
//        
//        for(int i=0;i<msrsRequestedArray.length;i++)
//        {
//            msrsRequestedNames[i] = msrsRequestedArray[i].getName();
//        }
//        
//        for(Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = originalData.entrySet().iterator();iterator.hasNext();)
//        {
//            
//          int count = 0;
//          Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
//          MeasureAggregator[] target = new MeasureAggregator[msrsRequestedArray.length];
//          ByteArrayWrapper keyWrapper = entry.getKey();
//          MeasureAggregator[] originalMeasureAggregators = entry.getValue();
//          for(int i=0;i<msrsRequestedNames.length;i++)
//          {
//              for(int j=0;j<msrs.length;j++)
//              {
//                  if(msrs[j].equals(msrsRequestedNames[i]))
//                  {//CHECKSTYLE:OFF    Approval No:Approval-335
//                    MeasureAggregator measureAggregator = originalMeasureAggregators[j];
//                    if(!measureAggregator.isFirstTime())
//                    {
//                        isNullMeasure[count]=false;
//                    }
//                    target[count] = measureAggregator;
//                    count++;
//                    break;
//                  }//CHECKSTYLE:ON
//              }
//          }
//          toReturn.put(keyWrapper, target);
//        }
//          return toReturn;
//    }
    
}
