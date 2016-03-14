/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOSmkmyhfVYpwuT2L6ZIwvFI/LYafLDnEFspGED5L2xqM2dpnLzHr0ZdEsGlc+TJaxVTZ
5kisVXLBzDublc8l0Xqq9DdqYcCFj23/xhV3cPJ7T5LDWqVs9ntmO0rwU88cVA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp;

/**
 * @author R00900208
 *
 */
public enum CalCExpressionName 
{
    
    /**
     * Types of calculated expression defined.
     */
    ADD("+"),
    /**
     * DIVIDE.
     */
    DIVIDE("/"),
    /**
     * NEGATIVE.
     */
    NEGATIVE("-"),
    /**
     * SUBSTRACT.
     */
    SUBSTRACT("-"),
    /**
     * MULTIPLY.
     */
    MULTIPLY("*"),
    /**
     * PARENTHESIS.
     */
    PARENTHESIS("()"),
    /**
     * MDX Statement
     * 
     */
    IIf("IIf"),
    /**
     * Equal
     */
    EQUAL("=");
    
    /**
     * name.
     */
    private String name;
    
    private CalCExpressionName(String name)
    {
        this.name = name;
    }
    
    /**
     * getName.
     * @return String
     */
    public String getName()
    {
        return name;
    }

}
