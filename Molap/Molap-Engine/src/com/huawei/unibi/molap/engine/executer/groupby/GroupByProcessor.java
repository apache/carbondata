/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7NSaFATtI3oOCDLYP03gd9USss+tJkPW5O/XmT2Vmp8hYVxwQTEm9XyU/U/I/IfTVv6X
h6quSwsmNICgh6jKknYb3yiHtEfvGFHqgS1at5UnD2iwtHSDkzWPlSP0ONEh6Q==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.groupby;

import java.util.Arrays;
import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcExpressionResolverUtil;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.engine.executer.pagination.DataProcessor;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;
import com.huawei.unibi.molap.engine.schema.metadata.MeasureFilterProcessorModel;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * It will group by and aggregate measures by the dimension
 * @author R00900208
 *
 */
public class GroupByProcessor implements DataProcessor
{

    /**
     * topMeasureIndex
     */
    private int topMeasureIndex;
//    /**
//     * topNCount
//     */
//    private int topNCount;
//    /**
//     * topNType
//     */
//    private MolapTopNType topNType;
//    
//    
//    /**
//     * groupMaskedBytes
//     */
//    private byte[] groupMaskedBytes;
    
    /**
     * maskedBytes
     */
    private byte[] maskedBytes;
    
    /**
     * 
     */
    private GroupByHolder holder;
    
    /**
     * avgMsrIndex
     */
    private int avgMsrIndex;
    
    /**
     * aggName
     */
    private String aggName;
    
    /**
     * countMsrIndex
     */
    private int countMsrIndex;
    
    /**
     * 
     */
    private DataProcessor processor;
    
    
    /**
     * 
     */
    private boolean isCalculatedMsr;
    
    /**
     * Query measures
     */
    private Measure[] queryMsrs;
    
    /**
     * Calc function
     */
    private MolapCalcFunction calcFunction;
    
    /**
     * maskedBytesPos
     */
    private int[] maskedBytesPos;

    private MeasureFilterProcessorModel msrFilterProcessorModel;
    
    private KeyGenerator keyGenerator;
    
    private String cubeUniqueName;
    
    /**
     * 
     * @param dimIndexes
     * @param topMeasureIndex
     * @param topNCount
     * @param topNType
     */
    public GroupByProcessor(DataProcessor processor)
    {
        this.processor = processor;
       
    }

    
    public GroupByProcessor(DataProcessor processor, MeasureFilterProcessorModel msrFilterProcessorModel)  
    {
        this.processor = processor;
        this.msrFilterProcessorModel=msrFilterProcessorModel; 
    }


    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException
    {
//        this.groupMaskedBytes = model.getGroupMaskedBytes();
        if(null!=msrFilterProcessorModel)
        {
            this.maskedBytes = msrFilterProcessorModel.getMaskedBytes();
            this.maskedBytesPos = msrFilterProcessorModel.getMaskedBytesPos();
            
        }
        else
        {
        this.maskedBytes = model.getMaskedBytes();
        this.maskedBytesPos = model.getTopNMaskedBytesPos();
        }
        this.topMeasureIndex = model.getTopMeasureIndex();
//        this.topNCount = model.getTopNCount();
//        this.topNType = model.getTopNType();
        this.avgMsrIndex = model.getAvgMsrIndex();
        this.countMsrIndex = model.getCountMsrIndex();
        this.aggName = model.getAggName();
        this.isCalculatedMsr = model.isTopCountOnCalcMeasure();
        this.queryMsrs = model.getQueryMsrs();

        if(isCalculatedMsr)
        {
            int calcMsrIndex = topMeasureIndex-queryMsrs.length;
            calcFunction = MolapCalcExpressionResolverUtil.createCalcExpressions(model.getCalculatedMeasures()[calcMsrIndex].getExp(), Arrays.asList(queryMsrs));
        }
        if(countMsrIndex < 0)
        {
            countMsrIndex = 0;
        }
        if(topMeasureIndex < 0)
        {
            topMeasureIndex = 0;
        }
        keyGenerator = model.getKeyGenerator();
        List<InMemoryCube> slices = model.getSlices();
        if(null != slices && slices.size() > 0 && null != slices.get(0))
        {
            cubeUniqueName = slices.get(0).getCubeUniqueName();
        }
        holder = new GroupByHolder(maskedBytes, topMeasureIndex, aggName, countMsrIndex, avgMsrIndex,isCalculatedMsr,queryMsrs,
                calcFunction,maskedBytesPos,keyGenerator,cubeUniqueName);
        if(processor != null)
        {
            processor.initModel(model); 
        }
    }
    
    /**
     * Add row to processor.
     * @param row
     * @param aggregators
     * @throws MolapPaginationException 
     */
    private void addRow(byte[] row,MeasureAggregator[] aggregators) throws MolapPaginationException
    {
        if(!holder.addRow(row,aggregators))
        {
            processor.processGroup(holder);
            holder = new GroupByHolder(maskedBytes, topMeasureIndex, aggName, countMsrIndex, avgMsrIndex,isCalculatedMsr
                    ,queryMsrs,calcFunction,maskedBytesPos,keyGenerator,cubeUniqueName);
            holder.addRow(row,aggregators);
        }
    }
    
    

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException
    {
       addRow(key, measures);
    }



    @Override
    public void finish() throws MolapPaginationException
    {
        if(holder.getRows().size() > 0)
        {
            processor.processGroup(holder);
        }
        
        processor.finish();
    }


    @Override
    public void processGroup(GroupByHolder groupByHolder)
    {
       //No implementation is required.
        
    }
}
