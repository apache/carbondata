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
package com.huawei.datasight.molap.spark.util;


/**
 * Its a temporary class for make the demo work
 * @author R00900208
 *
 */
public class MolapDirectQueryUtil
{
	
//	/**
//	 * Get the right table splits depends on the start keys.
//	 * @param conf
//	 * @param aggModel
//	 * @return
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	public static List<TableFileSplit> getSplits(Configuration conf,MultiDKeyAggModel aggModel) throws IOException,InterruptedException 
//	{
//		String rootDir = conf.get("mapred.hbase.rootdir", "hdfs://master:54310/opt/hbase2");
//		String table = aggModel.getFactTable();
//		Path srcPath = new Path(rootDir+"/"+table);
//		System.out.println("Choosen fact table path : "+srcPath.toString());
//		HTable hTable = new HTable(conf,table);
//		
//		List<byte[]> st = aggModel.getRegionStartKeys();
//	
//		
//		FileSystem fileSystem = srcPath.getFileSystem(conf);
//		List<FileStatus> fileStatus = getFileStatus(fileSystem, srcPath);
//		List<TableFileSplit> inputSplits = new ArrayList<TableFileSplit>();
//		NavigableMap<HRegionInfo, ServerName> map = hTable.getRegionLocations();
//		hTable.close();
//		List<String> paths = new ArrayList<String>();
//		Set<String> list = new HashSet<String>();
//		for(FileStatus fs : fileStatus)
//		{
//			for(HRegionInfo hRegionInfo : map.keySet())
//			{	
//		    	if(fs.getPath().toString().contains(hRegionInfo.getEncodedName()))
//		    	{
//		    		byte[] rst = hRegionInfo.getStartKey();
//		    		if(findStartKey(rst, st))
//		    		{
//		            	BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fs, 0, fs.getLen());
//		            	
//		            	for(BlockLocation  blockLocation: blockLocations)
//		            	{
//		            		String[] hosts = blockLocation.getHosts();
//		            		for(String host : hosts)
//		            		{
//		            			list.add(host);
//		            		}
//		            	}
//		            	//Right now we are creating the splits for each region file.but need to handle if
//		            	//the region have multiple files.
//		            	paths.add(fs.getPath().toString());
//		            	if(paths.size() > 0)
//		            	{
//		            		TableFileSplit inputSplit = new TableFileSplit();
//		            		inputSplit.setAggModel(aggModel);
//		            		inputSplit.setLocations(new ArrayList<String>(list));
//		            		inputSplit.setRegionName(hRegionInfo.getEncodedName());
//		            		inputSplit.setPaths(paths);
//		            		inputSplits.add(inputSplit);
//		            		paths = new ArrayList<String>();
//		            		list = new HashSet<String>();
//		            	}
//		        		System.out.println(fs.getPath());
//		    		}
//		    		break;
//		    	}
//			}
//			
//		}
//		return inputSplits;
//	}
//	
//	/**
//	 * whether startkey is present or not.
//	 * @param startKey
//	 * @param stKeys
//	 * @return
//	 */
//	private static boolean findStartKey(byte[] startKey,List<byte[]> stKeys)
//	{
//		
//		for(byte[] stKey : stKeys)
//		{
//			if(Bytes.compareTo(startKey, stKey) == 0)
//			{
//				return true;
//			}
//		}
//		return false;
//	}
	
	/**
	 * Get all Files present in the hbase table folder of hdfs.
	 * @param fileSystem
	 * @param path
	 * @return
	 * @throws IOException
	 */
//	public static List<FileStatus> getFileStatus(FileSystem fileSystem ,Path path) throws IOException
//	{
//		List<FileStatus> fileList = new ArrayList<FileStatus>();
//		 FileStatus[] fileStatus = fileSystem.listStatus(path,new PathFilter() {
//				@Override
//				public boolean accept(Path path) 
//				{
//				if (".tmp".contains(path.getName())
//						|| ".regioninfo".contains(path.getName())
//						|| path.getName().contains(".tableinfo")
//						|| path.getName().contains(".oldlogs")
//						|| path.getName().contains(".logs")
//						|| path.getName().startsWith("val"))
//					{
//						return false;
//					}
//					return true;
//				}
//			});
//	        for(FileStatus status : fileStatus)
//	        {
//	        	if(status.isDir())
//	        	{
//	        		fileList.addAll(getFileStatus(fileSystem, status.getPath()));
//	        	}
//	        	else
//	        	{
//	        		fileList.add(status) ;
//	        	}
//	        	
//	        }
//		return fileList;
//	}

//	public static void main(String[] args) throws Exception 
//	{
//		Configuration conf = new Configuration();
//		conf.setBoolean("hbase.defaults.for.version.skip",true);
//        conf.addResource(new Path("G:/core-site.xml"));
//        conf.addResource(new Path("G:/hdfs-site.xml"));
//        conf.setStrings("hbase.zookeeper.quorum", "master");
//        conf.setInt("hbase.zookeeper.property.clientPort",2181);
//        
//        String sql = "select City,sum(Quantity) from SteelWheelsSales ";
////        MolapProperties.getInstance().addProperty("molap.storelocation", "G:/imlib/store");
//        
//        MolapProperties.getInstance().addProperty("molap.storelocation", "hdfs://master:54310/opt/ravi/store");
//        
//        SimpleQueryParser parser = new SimpleQueryParser();
//        String tableName = parser.getTableName(sql);
//        MolapQueryUtil.createDataSource("G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml",null);
//        Cube cube =  MolapMetadata.getInstance().getCubeWithCubeName(tableName);
////        Cube cube = MolapQueryUtil.loadSchema("G:/SVN/NSE V300R007C10_Naga/Core/Molap/IntegrationTest/DETAIL_UFDR_Streaming.xml", null, tableName);
//		MolapQueryPlan plan = parser.parseQuery(sql, tableName, cube);
//        
//		MolapQueryExecutorModel parseQuery = MolapQueryUtil.createModel(plan, cube);//MolapQueryUtil.parseQuery( plan, "G:/SVN/NSE V300R007C10_Naga/Core/Molap/IntegrationTest/DETAIL_UFDR_Streaming.xml");
//		
//		
//		MolapExecutor molapExecutor = MolapQueryUtil.getMolapExecutor(parseQuery.getCube());
//		molapExecutor.execute(parseQuery);
//		System.out.println(parseQuery.gethIterator().getKeys());
//		System.out.println(parseQuery.gethIterator().getValues());
////		List<TableFileSplit> splits = getSplits(conf, aggModels);
////		System.out.println(splits);
////		MolapSparkQueryExecutor executor = new MolapSparkQueryExecutor(aggModels);
////		for (int i = 0; i < splits.size(); i++) 
////		{
////			TableFileSplit split = splits.get(i);
////			Scan scan =  executor.getScan(conf, split.getRegionName(),aggModels.getCube(),aggModels.getFactTable());
////			BigDataFileIterator iter = new BigDataFileIterator(split.getPaths(),conf,scan,1000);
////			List<HBaseResultRow> processBatchData = executor.processBatchData(iter, split.getAggModel().getMeasureOffsets(),10);
////			Pair<List<MolapKey>, List<MolapValue>> processData = MolapQueryUtil.getExecutor(parseQuery).processData(processBatchData, aggModels);
////			System.out.println(processData.getKey());
////			System.out.println(processData.getValue());
////		}
//	}
	

}
