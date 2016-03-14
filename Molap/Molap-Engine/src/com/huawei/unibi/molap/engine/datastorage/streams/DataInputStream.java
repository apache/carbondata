/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBsDLVq/YKyh6HtHHa1yn3o0WSvKoE2AYc3ABFIxK4P/du2tStgLT2mLgioNW9/3BKOFV
7/8cebfCrnQDbYT5LwLufN6xf3VrmVW9VlGpoBNDNvsgdgsjtkkn4G/y3cH6nA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.datastorage.streams;

import java.util.List;

import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :25-Jun-2013
 * FileName : DataInputStream.java
 * Class Description : 
 * Version 1.0
 */
public interface DataInputStream
{
    void initInput();

    void closeInput();

    ValueCompressionModel getValueCompressionMode();
    
    List<LeafNodeInfoColumnar> getLeafNodeInfoColumnar();
    
    List<LeafNodeInfo> getLeafNodeInfo();
    
    Pair getNextHierTuple();
    
    byte[] getStartKey();
}
