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

/**
 *
 */
package org.carbondata.query.writer;

import java.util.concurrent.Callable;

import org.carbondata.query.schema.metadata.DataProcessorInfo;

/**
 * Project Name  : Carbon
 * Module Name   : CARBON Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : ResultWriter.java
 * Description   : This is the abstract class for writing the result to a file.
 * Class Version  : 1.0
 */
public abstract class ResultWriter implements Callable<Void> {
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
