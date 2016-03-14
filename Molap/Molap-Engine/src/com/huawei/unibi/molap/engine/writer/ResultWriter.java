/**
 * 
 */
package com.huawei.unibi.molap.engine.writer;

import java.util.concurrent.Callable;

import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : ResultWriter.java
 * Description   : This is the abstract class for writing the result to a file.
 * Class Version  : 1.0
 */
public abstract class ResultWriter implements Callable<Void>
{
    /**
     * Pagination model
     */
    protected DataProcessorInfo dataProcessorInfo;
    
//    /**
//     * uniqueDimension
//     */
//    private Dimension[] uniqueDimension;
//
//    /**
//     * This method removes the duplicate dimensions.
//     */
//    protected void updateDuplicateDimensions()
//    {
//        List<Dimension> dimensions = new ArrayList<Dimension>(20);
//
//        Dimension[] queryDimension = dataProcessorInfo.getQueryDims();
//
//        for(int i = 0;i < queryDimension.length;i++)
//        {
//            boolean found = false;
//            // for each dimension check against the queryDimension.
//            for(Dimension dimension : dimensions)
//            {
//                if(dimension.getOrdinal() == queryDimension[i].getOrdinal())
//                {
//                    found = true;
//                }
//            }
//            if(!found)
//            {
//                dimensions.add(queryDimension[i]);
//            }
//        }
//        this.uniqueDimension = dimensions.toArray(new Dimension[dimensions.size()]);
//    }

}
