package com.huawei.unibi.molap.engine.evaluators.logical;

import com.huawei.unibi.molap.engine.evaluators.FilterEvaluator;
import com.huawei.unibi.molap.engine.schema.metadata.FilterEvaluatorInfo;

public abstract class AbstractLogicalFilterEvaluator implements FilterEvaluator
{
    protected FilterEvaluator leftEvalutor;
    
    protected FilterEvaluator rightEvalutor;
    
    public AbstractLogicalFilterEvaluator(FilterEvaluator leftEvalutor, FilterEvaluator rightEvalutor)
    {
        this.leftEvalutor=leftEvalutor;
        this.rightEvalutor=rightEvalutor;
    }
    @Override
    public void resolve(FilterEvaluatorInfo info)
    {
        
    }
    
    public FilterEvaluator getLeft()
    {
        return leftEvalutor;
    }
    
    public FilterEvaluator getRight()
    {
        return rightEvalutor;
    }
}
