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
package com.huawei.datasight.molap.spark.splits;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.io.Writable;

import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;

/**
 * @author R00900208
 *
 */
public class LoadSplit implements Serializable,Writable
{
	
	 private static final LogService LOGGER = LogServiceFactory
	            .getLogService(LoadSplit.class.getName());
	/**
	 * 
	 */
	private static final long serialVersionUID = 4438846634046742148L;

	private long jobId;
	
	private BlockingQueue<String> filelistQueue;
	
	private Cube cube;
	
	private String tableName;

	private int clientCount;
	
	private List<String> locations;
	
	public LoadSplit()
	{
		
	}
	
	public LoadSplit(long jobId, BlockingQueue<String> filelistQueue,
			Cube cube, String tableName,int clientCount,List<String> locations) 
	{
		this.jobId = jobId;
		this.filelistQueue = filelistQueue;
		this.cube = cube;
		this.tableName = tableName;
		this.clientCount = clientCount;
		this.locations = locations;
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		jobId = in.readLong();
		filelistQueue = (BlockingQueue)readObject(in);
		cube = (Cube)readObject(in);
		tableName = readString(in);
		clientCount = in.readInt();
		locations = (List)readObject(in);
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeLong(jobId);
		writeObject(out, filelistQueue);
		writeObject(out, cube);
		writeString(tableName, out);
		out.writeInt(clientCount);
		writeObject(out, locations);
	}
	
	private void writeObject(DataOutput out,Object object) throws IOException
	{
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		ObjectOutputStream objStrm = new ObjectOutputStream(stream);
		objStrm.writeObject(object);
		objStrm.close();
		stream.close();
		byte[] byteArray = stream.toByteArray();
		out.writeInt(byteArray.length);
		out.write(byteArray);
	}
	
	private Object readObject(DataInput in) throws IOException
	{
		Object obj = null;
		int arraySize = in.readInt();
		byte[] objArr = new byte[arraySize];
		in.readFully(objArr);
		ByteArrayInputStream stream = new ByteArrayInputStream(objArr);
		ObjectInputStream objStrm = new ObjectInputStream(stream);
		try 
		{
			obj = objStrm.readObject();
		} 
		catch (ClassNotFoundException e) 
		{
			// TODO Auto-generated catch block
//			e.printStackTrace();
			LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());
		}
		return obj;
	}
	
	private void writeString(String string,DataOutput out) throws IOException
	{
		byte[] bytes = string.getBytes(Charset.defaultCharset());
		out.writeInt(bytes.length);
		out.write(bytes);
	}
	
	private String readString(DataInput in) throws IOException
	{
		byte[] bytes = new byte[in.readInt()];
		in.readFully(bytes);
		return new String(bytes,Charset.defaultCharset());
	}

	/**
	 * @return the jobId
	 */
	public long getJobId() 
	{
		return jobId;
	}

	/**
	 * @return the filelistQueue
	 */
	public BlockingQueue<String> getFilelistQueue() 
	{
		return filelistQueue;
	}

	/**
	 * @return the cube
	 */
	public Cube getCube() 
	{
		return cube;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() 
	{
		return tableName;
	}

	/**
	 * @return the locations
	 */
	public List<String> getLocations() 
	{
		return locations;
	}

	/**
	 * @return the clientCount
	 */
	public int getClientCount() 
	{
		return clientCount;
	}

	/**
	 * @param clientCount the clientCount to set
	 */
	public void setClientCount(int clientCount) 
	{
		this.clientCount = clientCount;
	}

}
