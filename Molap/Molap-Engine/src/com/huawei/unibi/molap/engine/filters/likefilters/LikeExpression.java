package com.huawei.unibi.molap.engine.filters.likefilters;

import java.io.Serializable;

public class LikeExpression  implements Serializable {

	/**
     * 
     */
    private static final long serialVersionUID = 2647557717408470809L;
    private String expressionName;
    private boolean notExpression;
    public boolean isNotExpression()
    {
        return notExpression;
    }
    public void setNotExpression(boolean notExpression)
    {
        this.notExpression = notExpression;
    }

	public String getExpressionName() {
		return expressionName;
	}

	public void setExpressionName(String expressionName) {
		this.expressionName = expressionName;
	}

	
}
