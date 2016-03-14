package com.huawei.datasight.molap.datastats.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

/**
 * this class will read iterator and find out distinct dimension data
 * 
 * @author A00902717
 *
 */
public class ResultAnalyzer
{
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(ResultAnalyzer.class.getName());

	private Dimension masterDimension;

	public ResultAnalyzer(Dimension masterDimension)
	{
		this.masterDimension = masterDimension;
	}

	/**
	 * result will be set in distinctOfMasterDim
	 * @param rowIterator
	 * @param distinctOfMasterDim 
	 * @param cardinalities
	 * @return result: It will have each dimension and its distinct value
	 */
	public void analyze(
			MolapIterator<RowResult> rowIterator, ArrayList<Level> dimensions, Map<Integer,Integer> distinctOfMasterDim)
	{
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Scanning query Result:" + System.currentTimeMillis());

		//Object:master data
		//HashMap<Integer, HashSet<Object> -> slaveDimension,slavesData for each master data
		HashMap<Object, HashMap<Integer, HashSet<Object>>> datas = new HashMap<Object, HashMap<Integer, HashSet<Object>>>(100);
		
		while (rowIterator.hasNext())
		{
			RowResult rowResult = rowIterator.next();
			Object[] results = rowResult.getKey().getKey();
			int columnCounter = 0;
			HashMap<Integer, HashSet<Object>> masterDatas = null;
			for (Level dimension : dimensions)
			{
				Object resData = results[columnCounter++];

				if (dimension.getOrdinal() == masterDimension.getOrdinal())
				{
					masterDatas = datas.get(resData);
					if (null == masterDatas)
					{
						masterDatas = new HashMap<Integer, HashSet<Object>>(dimensions.size());
						datas.put(resData, masterDatas);
					}
					continue;
				}
				if(masterDatas!=null)
				{
				HashSet<Object> slaveData = masterDatas.get(dimension
						.getOrdinal());
				if (null == slaveData)
				{
					slaveData = new HashSet<Object>(dimensions.size());
					masterDatas.put(dimension.getOrdinal(), slaveData);
				}
				slaveData.add(resData);
				}
			}
		
		}
		calculateAverage(datas,distinctOfMasterDim,dimensions);

	}

	/**
	 * for each dimension count no of values and average it and return the result
	 * 
	 * @param datas
	 * @param distinctOfMasterDim 
	 * @param dimensions 
	 * @return
	 */
	private void calculateAverage(
			HashMap<Object, HashMap<Integer, HashSet<Object>>> datas, Map<Integer,Integer> distinctOfMasterDim, ArrayList<Level> allDimensions)
	{

		//Step 1 merging all slave data
		Set<Entry<Object, HashMap<Integer, HashSet<Object>>>> sampleDatas = datas
				.entrySet();
		Iterator<Entry<Object, HashMap<Integer, HashSet<Object>>>> sampleDataItr = sampleDatas
				.iterator();
		while (sampleDataItr.hasNext())
		{
			Entry<Object, HashMap<Integer, HashSet<Object>>> sampleData = sampleDataItr
					.next();
			HashMap<Integer, HashSet<Object>> slavesData = sampleData
					.getValue();
			Set<Entry<Integer, HashSet<Object>>> slaves = slavesData.entrySet();
			Iterator<Entry<Integer, HashSet<Object>>> slaveItr = slaves
					.iterator();
			while (slaveItr.hasNext())
			{
				Entry<Integer, HashSet<Object>> slave = slaveItr.next();
				int slaveOrdinal = slave.getKey();
				HashSet<Object> slaveData = slave.getValue();
				Integer existingVal = distinctOfMasterDim.get(slaveOrdinal);

				if (null==existingVal||0 == existingVal)
				{
					distinctOfMasterDim.put(slaveOrdinal,slaveData.size());

				}
				else
				{
					distinctOfMasterDim.put(slaveOrdinal, (existingVal+ slaveData.size()));

				}

			}
		}

		//step 2 calculating average
		Iterator<Level> itr = allDimensions.iterator();
		while (itr.hasNext())
		{
			Level level = itr.next();
			if (level.getOrdinal() == masterDimension.getOrdinal())
			{
				continue;
			}
			Integer distinctData = distinctOfMasterDim.get(level.getOrdinal());
			
			Double avg=0.0;
			if(null==distinctData)
			{
				distinctData=0;
			}
			
			if(sampleDatas.size()>0)
			{
				avg = Double.valueOf(Math.round((double) distinctData
						/ sampleDatas.size()));	
			}
			
			distinctOfMasterDim.put(level.getOrdinal(),avg.intValue() == 0 ? 1
					: avg.intValue());

		}
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Finished Scanning query Result:" + System.currentTimeMillis());

	}

}
