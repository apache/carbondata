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

package com.huawei.datasight.molap.datastats.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.eigenbase.xom.XOMException;
import org.junit.Before;
import org.junit.Test;

import com.huawei.datasight.molap.autoagg.util.CommonUtil;
import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.datasight.molap.util.TestUtil;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.util.MolapUtil;

public class CommonUtilTest
{
	static MolapDef.Schema schema;
	static MolapDef.Cube cube;

	static String schemaName;
	static String cubeName;
	static String factTable;

	static String dataPath;
	static String baseMetaPath;
	private LoadModel loadModel;
	private String metaPath;

	@Before
	public void setUpBeforeClass() throws Exception
	{
		File file = new File("../../libraries/testData/Molap-Aggregation/store/");
		String basePath = file.getCanonicalPath() + "/";
		metaPath = basePath + "schemas/default/carbon/metadata";

		schema = CommonUtil.readMetaData(metaPath).get(0);
		cube = schema.cubes[0];
		schemaName = schema.name;
		cubeName = cube.name;
		factTable = "carbon";
		// dataPath="src/test/resources/store/store";
		// baseMetaPath="src/test/resources/store/schemas";
		dataPath = basePath + "store";
		baseMetaPath = basePath + "schemas/default/carbon";

		dataPath = basePath + "store";
		baseMetaPath = basePath + "schemas/default/carbon";
		loadModel = TestUtil.createLoadModel(schemaName, cubeName, schema,
				cube, dataPath, baseMetaPath);

	}

	@Test
	public void testsetListOfValidSlices_updateLoad()
	{
		new MockUp<LoadMetadataDetails>()
		{

			@Mock
			public String getLoadStatus()
			{
				return MolapCommonConstants.MARKED_FOR_UPDATE;
			}

		};

		CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
		Assert.assertTrue(true);
	}

	@Test
	public void testsetListOfValidSlices_partitalSuccess()
	{
		new MockUp<LoadMetadataDetails>()
		{

			@Mock
			public String getLoadStatus()
			{
				return MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS;
			}

		};

		CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
		Assert.assertTrue(true);
	}
	
	@Test
	public void testsetListOfValidSlices_throwException()
	{
		new MockUp<java.io.DataInputStream>()
		{

			@Mock
			public void close() throws IOException
			{
				throw new IOException();
			}

		};

		CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
		Assert.assertTrue(true);
	}

	@Test
	public void testsetListOfValidSlices_IOException()
	{
		new MockUp<FileFactory>()
		{

			@Mock
			public boolean isFileExist(String filePath, FileType fileType)
					throws IOException
			{
				throw new IOException();
			}

		};

		CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
		Assert.assertTrue(true);

	}
	
	@Test
	public void testReadMetaData_FileNotExist()
	{
		
		Assert.assertNull(CommonUtil.readMetaData("test/resource"));
	}
	
	@Test
	public void testReadMetaData_nullSource()
	{
		Assert.assertNull(CommonUtil.readMetaData(null));
	}
	
	@Test
	public void testReadMetaData_throwIOException()
	{
		new MockUp<java.io.DataInputStream>()
		{

			@Mock
			public void readFully(byte b[]) throws IOException
			{
				throw new IOException();
			}

		};
		Assert.assertNull(CommonUtil.readMetaData(metaPath));
	}
	
	@Test
	public void testReadMetaData_throwXOMException()
	{
		new MockUp<CommonUtil>()
		{

			@Mock
			public Schema parseStringToSchema(String schema) throws XOMException
			{
				throw new XOMException();
			}

		};
		Assert.assertNull(CommonUtil.readMetaData(metaPath));
	}
	
	
}
