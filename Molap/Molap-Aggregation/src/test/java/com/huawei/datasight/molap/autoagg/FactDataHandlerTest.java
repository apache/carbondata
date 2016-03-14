package com.huawei.datasight.molap.autoagg;

import java.io.File;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.junit.Before;
import org.junit.Test;

import com.huawei.datasight.molap.autoagg.util.CommonUtil;
import com.huawei.datasight.molap.datastats.load.FactDataHandler;
import com.huawei.datasight.molap.datastats.load.LevelMetaInfo;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.datasight.molap.util.TestUtil;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.util.MolapProperties;

public class FactDataHandlerTest
{

	static MolapDef.Schema schema;
	static MolapDef.Cube cube;

	static String schemaName;
	static String cubeName;
	static String factTable;

	static String dataPath;
	static String baseMetaPath;
	private LoadModel loadModel;

	@Before
	public void setUpBeforeClass() throws Exception
	{
		try
		{

			File file = new File(
					"../../libraries/testData/Molap-Aggregation/store/");
			String basePath = file.getCanonicalPath() + "/";
			String metaPath = basePath + "schemas/default/carbon/metadata";

			MolapProperties.getInstance().addProperty("molap.storelocation",
					basePath + "store");
			MolapProperties.getInstance().addProperty("molap.number.of.cores",
					"4");
			MolapProperties.getInstance().addProperty("molap.agg.benefitRatio",
					"10");
			MolapProperties.getInstance().addProperty(
					Preference.AGG_LOAD_COUNT, "2");
			MolapProperties.getInstance().addProperty(
					Preference.AGG_FACT_COUNT, "2");
			MolapProperties.getInstance().addProperty(Preference.AGG_REC_COUNT,
					"5");
			schema = CommonUtil.readMetaData(metaPath).get(0);
			cube = schema.cubes[0];
			schemaName = schema.name;
			cubeName = cube.name;
			factTable = "carbon";
			// dataPath="src/test/resources/store/store";
			// baseMetaPath="src/test/resources/store/schemas";
			dataPath = basePath + "store";
			baseMetaPath = basePath + "schemas/default/carbon";
			loadModel = TestUtil.createLoadModel(schemaName, cubeName, schema,
					cube, dataPath, baseMetaPath);
			CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
			CommonUtil.fillSchemaAndCubeDetail(loadModel);
		}
		catch (Exception e)
		{

		}

	}

	@Test
	public void testFactDataHandler()
	{
		
		 new MockUp<MolapMetadata.Cube>()
	        {

	            @Mock
	            public String getMode()
	            {
	                return "test";
	            }

	        };
	    MolapProperties.getInstance().addProperty(MolapCommonConstants.MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY, "false");
		MolapMetadata.getInstance().loadCube(schema,schema.name, cube.name, cube);
		Cube metaCube = MolapMetadata.getInstance().getCube(
				schema.name +"_"+ cube.name);
		String levelMetaPath = dataPath
				+ "/default_0/carbon_0/RS_0/carbon/Load_0";
		MolapFile file = FileFactory.getMolapFile(levelMetaPath,
				FileFactory.getFileType(levelMetaPath));

		LevelMetaInfo levelMetaInfo = new LevelMetaInfo(file,
				loadModel.getTableName());
		FactDataHandler factDataHandler = new FactDataHandler(metaCube,
				levelMetaInfo, loadModel.getTableName(), 1, null);
		Assert.assertTrue(true);

	}
}
