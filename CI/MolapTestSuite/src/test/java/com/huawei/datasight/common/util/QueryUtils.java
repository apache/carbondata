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

package com.huawei.datasight.common.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.eigenbase.xom.XOMException;

import com.huawei.datasight.common.cubemeta.CubeMetadata;
import com.huawei.datasight.molap.query.MolapQueryPlan;
import com.huawei.datasight.molap.spark.util.MolapQueryUtil;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.util.MolapProperties;

public class QueryUtils {

	public static MolapQueryExecutorModel createQueryModel(MolapQueryPlan logicalPlan) throws XOMException, IOException, ClassNotFoundException
	{
		CubeMetadata cubeMeta = CommonUtils.readCubeMetaDataFile(logicalPlan.getSchemaName(), logicalPlan.getCubeName());
		Schema schema = CommonUtils.createSchemaObjectFromXMLString(cubeMeta.getSchema());
		MolapMetadata.getInstance().loadSchema(schema);
		MolapMetadata.Cube cube = MolapMetadata.getInstance().getCube(logicalPlan.getSchemaName()+"_"+logicalPlan.getCubeName());
		
	    MolapQueryExecutorModel molapQueryModel = MolapQueryUtil.createModel(logicalPlan, 
	    		schema, cube, cubeMeta.getDataPath(), 
	    		cubeMeta.getPartitioner().partitionCount());
	    MolapQueryUtil.updateMolapExecuterModelWithLoadMetadata(molapQueryModel);
	    MolapQueryUtil.setPartitionColumn(molapQueryModel, cubeMeta.getPartitioner().partitionColumn());
	    
	    return molapQueryModel;
	}

	public static MolapIterator<RowResult> runQuery(MolapQueryExecutorModel molapQueryModel) throws QueryExecutionException, XOMException, IOException, ClassNotFoundException
	{
	    CubeMetadata cubeMeta = CommonUtils.readCubeMetaDataFile(
	    		molapQueryModel.getCube().getSchemaName(), molapQueryModel.getCube().getOnlyCubeName());
		String part = 0+"";
	    molapQueryModel.setPartitionId(part);
	    List<String> listOfLoadFolders=MolapQueryUtil.getSliceLoads(molapQueryModel, cubeMeta.getDataPath(), part);
	    listOfLoadFolders.add("Load_0");
	    List<String> listOfUpdatedLoadFolders= molapQueryModel.getListOfValidUpdatedSlices();
	    String molapBasePath = MolapProperties.getInstance().getProperty("molap.storelocation");
	    MolapProperties.getInstance().addProperty("molap.storelocation", cubeMeta.getDataPath());
	    MolapProperties.getInstance().addProperty("molap.cache.used", "false");
	    MolapQueryUtil.createDataSource(0, CommonUtils.createSchemaObjectFromXMLString(cubeMeta.getSchema()), 0+"",listOfLoadFolders,listOfUpdatedLoadFolders,molapQueryModel.getFactTable(),0);
	    Cube cube = MolapMetadata.getInstance().getCube(molapQueryModel.getCube().getSchemaName() + '_' + part + '_' + molapQueryModel.getCube().getOnlyCubeName() + '_' + part);
	    MolapProperties.getInstance().addProperty("molap.is.columnar.storage", "true");
	    MolapProperties.getInstance().addProperty("molap.dimension.split.value.in.columnar", "1");
	    MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits", "true");
	    MolapProperties.getInstance().addProperty("is.int.based.indexer", "true");
	    MolapProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
	    MolapProperties.getInstance().addProperty("high.cardinality.value", "100000");
	    MolapProperties.getInstance().addProperty("is.compressed.keyblock", "false");
	    MolapProperties.getInstance().addProperty("molap.leaf.node.size", "120000");
	    molapQueryModel.setCube(cube);
	    MolapIterator<RowResult> rowIterator=MolapQueryUtil.getQueryExecuter(molapQueryModel.getCube(), molapQueryModel.getFactTable()).execute(molapQueryModel);
	    MolapProperties.getInstance().addProperty("molap.storelocation", molapBasePath);
	    return rowIterator;
	}
	
	public static void validateQueryOutput(MolapIterator<RowResult> rowIterator, List<String> expectedOutput) throws Exception
	{
		int expectedCount = expectedOutput.size();
		while (rowIterator.hasNext())
		{
			RowResult rowResult = rowIterator.next();
			Object[] key = rowResult.getKey().getKey();
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<key.length;i++)
			{
				sb.append(key[i]);
				if(i != key.length - 1)
				{
					sb.append(",");
				}
			}
			if(expectedOutput.remove(sb.toString()))
				continue;
			else
				throw new Exception("Invalid unexpected Data : " + sb.toString());
		}
		if(expectedOutput.size() != 0)
			throw new Exception("Invalid result count : Expected Output Row Count = " + expectedCount);
	}
	
	public static void validateTimestampQueryOutput(MolapIterator<RowResult> rowIterator, List<String> expectedOutput) throws Exception
	{
		int expectedCount = expectedOutput.size();
		while (rowIterator.hasNext())
		{
			RowResult rowResult = rowIterator.next();
			Object[] key = rowResult.getKey().getKey();
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<key.length;i++)
			{
				Date d = new Date((long)key[i] / 1000);
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(MolapProperties.getInstance().getProperty(
		                MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT));
				sb.append(simpleDateFormat.format(d));
				if(i != key.length - 1)
				{
					sb.append(",");
				}
			}
			if(expectedOutput.remove(sb.toString()))
				continue;
			else
				throw new Exception("Invalid unexpected Data : " + sb.toString());
		}
		if(expectedOutput.size() != 0)
			throw new Exception("Invalid result count : Expected Output Row Count = " + expectedCount);
	}
	
}
