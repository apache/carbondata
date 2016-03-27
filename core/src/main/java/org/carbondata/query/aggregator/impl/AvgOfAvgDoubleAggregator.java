/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090SI2vhQLZdN9xXYHEupl+nhkRgKlnd278HRtGfXkaEXPvEML++W+rNhsQQATOGLh7fNE
dMQGS+xtKYZbQzG/f9W5WuvVcGSOyC1WRBfsYwKXUO3qTMgVm3zWpPaZP9EA5Q==*/
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
package org.carbondata.query.aggregator.impl;

import java.nio.ByteBuffer;

import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

/**
 * Project Name NSE V3R7C00
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :13-May-2013 3:35:33 PM
 * FileName : AvgOfAvgAggregator.java
 * Class Description : This class will be used for aggregate tables. It is
 * overriding agg method. It will be used for getting the average of fact
 * count(average value)
 * Version 1.0
 */

public class AvgOfAvgDoubleAggregator extends AvgDoubleAggregator {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 6482976744603672084L;

    /**
     * Overloaded Aggregate function will be used for Aggregate tables because
     * aggregate table will have fact_count as a measure.
     *
     * @param newVal new value
     * @param index  index
     */
    @Override
    public void agg(CarbonReadDataHolder newVal, int index) {
        byte[] value = newVal.getReadableByteArrayValueByIndex(index);
        ByteBuffer buffer = ByteBuffer.wrap(value);
        double newValue = buffer.getDouble();
        double factCount = buffer.getDouble();
        aggVal += (newValue * factCount);
        count += factCount;
        firstTime = false;
    }

}
