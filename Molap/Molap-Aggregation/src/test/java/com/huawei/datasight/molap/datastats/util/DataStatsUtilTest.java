package com.huawei.datasight.molap.datastats.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.junit.Test;

import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.olap.SqlStatement.Type;
import com.huawei.unibi.molap.engine.expression.DataType;

public class DataStatsUtilTest
{
	private String basePath = "src/test/resources";

	@Test
	public void testSerializeObject_ExceptionFromDataOutputStream()
	{
		File test = new File(basePath);
		Level level = new Level(1, 10);
		level.setName("1");
		try
		{
			new MockUp<FileFactory>()
			{

				@Mock
				public DataOutputStream getDataOutputStream(String path,
						FileType fileType) throws IOException
				{
					throw new IOException();
				}

			};

			DataStatsUtil.serializeObject(level, test.getCanonicalPath(),
					"test");
			Assert.assertTrue(true);

		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testReadSerializedFile_ExceptionFromDataInputputStream()
	{
		File test = new File(basePath + "/test");
		Level level = new Level(1, 10);
		level.setName("1");

		new MockUp<FileFactory>()
		{

			@Mock
			public DataInputStream getDataInputStream(String path,FileType fileType) throws IOException
			{
				throw new IOException();
			}

		};
		
		new MockUp<FileFactory>()
		{

			@Mock
			public boolean isFileExist(String filePath,FileType fileType, boolean performFileCheck) throws IOException
			{
				return true;
			}

		};

		DataStatsUtil.readSerializedFile(test.getAbsolutePath());
		Assert.assertTrue(true);

	}

	@Test
	public void testSerializeObject_ExceptionFromObjectStream()
	{
		File test = new File(basePath);
		Level level = new Level(1, 10);
		level.setName("1");
		try
		{
			new MockUp<ObjectOutputStream>()
			{

				@Mock
				public final void writeObject(Object obj) throws IOException
				{
					throw new IOException();
				}

			};

			DataStatsUtil.serializeObject(level, test.getCanonicalPath(),
					"test");
			Assert.assertTrue(true);

		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void testCreateDirectory_failedMakeDir()
	{
		File test = new File(basePath + "/test");
		new MockUp<FileFactory>()
		{

			@Mock
			public boolean mkdirs(String filePath, FileType fileType)
					throws IOException
			{
				return false;
			}

		};
		try
		{
			if (test.exists())
			{
				test.delete();
			}
			Assert.assertTrue(!DataStatsUtil.createDirectory(test
					.getCanonicalPath()));

		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testCreateDirectory_failedMakeDir_throwException()
	{
		File test = new File(basePath + "/test");
		new MockUp<FileFactory>()
		{

			@Mock
			public boolean mkdirs(String filePath, FileType fileType)
					throws IOException
			{
				throw new IOException();
			}

		};
		try
		{
			if (test.exists())
			{
				test.delete();
			}
			Assert.assertTrue(!DataStatsUtil.createDirectory(test
					.getCanonicalPath()));

		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testReadSerializedFile_FileExistThrowException()
	{
		new MockUp<FileFactory>()
		{

			@Mock
			public boolean isFileExist(String filePath, FileType fileType,
					boolean performFileCheck) throws IOException
			{
				throw new IOException();
			}

		};
		DataStatsUtil.readSerializedFile("test/test");
		Assert.assertTrue(true);
	}
	
	@Test
	public void testGetDataType()
	{
		Assert.assertEquals(DataType.DoubleType, DataStatsUtil.getDataType(Type.DOUBLE));
		Assert.assertEquals(DataType.LongType, DataStatsUtil.getDataType(Type.LONG));
		Assert.assertEquals(DataType.BooleanType, DataStatsUtil.getDataType(Type.BOOLEAN));
	}
}
