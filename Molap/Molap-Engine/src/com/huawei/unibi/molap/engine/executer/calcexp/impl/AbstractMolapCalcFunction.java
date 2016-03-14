/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOWKIF8yylyDUXjxfZcD3aabUxswGzGGVG5joPLk1NNERZeX3wEZOJ/T9qIE2JJar75I3
FgJgasOUPeXz9vgELP/hKQu7VZVZ44fUuIfMSAne+Chu8gAn690wNtCHvOKcQA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

//import mondrian.mdx.MemberExpr;
//import mondrian.mdx.ResolvedFunCall;
//import mondrian.olap.Exp;
//import mondrian.olap.Literal;
//import mondrian.rolap.RolapHierarchy.RolapCalculatedMeasure;

//import com.huawei.unibi.molap.engine.executer.calcexp.CalCExpressionName;
//import com.huawei.unibi.molap.engine.executer.calcexp.CalcExpressionFactory;
//import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcExpressionResolverUtil;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;
import com.huawei.unibi.molap.olap.Exp;

/**
 * @author R00900208
 *
 */
public abstract class AbstractMolapCalcFunction implements MolapCalcFunction
{

    /**
     * 
     */
    private static final long serialVersionUID = -3407652955179817672L;

    protected MolapCalcFunction leftOperand;
    
    protected MolapCalcFunction rightOperand;
    
    protected boolean conditionValue;
    
    @Override
    public void compile(CalcExpressionModel model, Exp exp)
    {
//        ResolvedFunCall funCall = (ResolvedFunCall)exp;
        
//        Exp[] args = funCall.getArgs();
//        leftOperand = getOperand(args[0],model);
//        rightOperand = getOperand(args[1],model);
    }
    
    /**
     * @param args
     * @param i
     */
    protected MolapCalcFunction getOperand(Exp exp, CalcExpressionModel model)
    {
        Pair<MolapCalcFunction, Exp> pair = getMolapFunction(exp);
        if(pair.getKey() != null)
        {
            pair.getKey().compile(model, pair.getValue());
        }
        return pair.getKey();
    }

    /**
     * @param args
     * @param i
     * @param function
     * @return
     */
    private Pair<MolapCalcFunction, Exp> getMolapFunction(Exp exp)
    {
        MolapCalcFunction function = null;
//        if(exp instanceof ResolvedFunCall)
//        {
//            ResolvedFunCall newName = (ResolvedFunCall)exp;
//            CalCExpressionName calcExpr = MolapCalcExpressionResolverUtil.getCalcExpr(newName.getFunName());
//            function = CalcExpressionFactory.getInstance().getCalcFunction(calcExpr);
//        }
//        else if(exp instanceof MemberExpr)
//        {
//            MemberExpr expr = (MemberExpr)exp;
//            if(expr.getMember() instanceof RolapCalculatedMeasure)
//            {
//                RolapCalculatedMeasure calMsr = (RolapCalculatedMeasure)expr.getMember();
//                return getMolapFunction(calMsr.getExpression());
//            }
//            else
//            {
//                function = CalcExpressionFactory.getInstance().getSingleCalcFunction();
//            }
//            
//        }
//        else if(exp instanceof Literal)
//        {
//            function = new MolapConstCalcFunction();
//        }
        return new Pair<MolapCalcFunction, Exp>(function, exp);
    }
    
    
}
