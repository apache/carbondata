/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnPhfbxNTG9fxb2HSA0kk2XIbOSyB2Es+vd8aIBNzy2zw6r42bC6vrYXTgCzt4v8xQVm2
vCiD+MUBy5sK6pmSa5YCJgXswRkefBQElATSZlZBIBFDpQauppQer/nQFx8c1Q==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.dimension.load.command.invoker;

import com.huawei.unibi.molap.dimension.load.command.DimensionLoadCommand;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :14-Nov-2013 6:13:57 PM
 * FileName : DimensionLoadActionInvoker.java
 * Class Description :
 * Version 1.0
 */
public class DimensionLoadActionInvoker
{
    /**
     * Dimension Load
     */
    private DimensionLoadCommand dimensionLoadCommand;
    /**
     * 
     * 
     */
    public DimensionLoadActionInvoker(DimensionLoadCommand loadCommand)
    {
        this.dimensionLoadCommand = loadCommand;
    }
    
    /**
     * Executes the command
     * @throws Exception 
     * 
     *
     */
    public void execute() throws Exception
    {
        dimensionLoadCommand.execute();
    }

}

