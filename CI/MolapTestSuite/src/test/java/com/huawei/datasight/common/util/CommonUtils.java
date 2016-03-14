package com.huawei.datasight.common.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;

import org.apache.spark.sql.cubemodel.Partitioner;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

import com.huawei.datasight.common.cubemeta.CubeMetadata;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.util.MolapProperties;

public class CommonUtils {
	public static String parseXMLPathToSchemaString(String schemaXMLPath) throws XOMException, IOException
	{
		FileType fileType = FileFactory.getFileType(schemaXMLPath);
		InputStreamReader fileReader = new InputStreamReader(FileFactory.getDataInputStream(schemaXMLPath, fileType));
		BufferedReader bufReader = new BufferedReader(fileReader); 
		StringBuilder sb = new StringBuilder(); 
		String line = bufReader.readLine(); 
		while( line != null)
		{
			sb.append(line).append("\n"); 
			line = bufReader.readLine(); 
		} 
		bufReader.close();
		
		return sb.toString();
	}
	
	public static Schema createSchemaObjectFromXMLString(String schemaXML) throws XOMException 
	{
		Parser xmlParser = XOMUtil.createDefaultParser();
		ByteArrayInputStream baoi = new ByteArrayInputStream(schemaXML.getBytes());
		DOMWrapper defin = xmlParser.parse(baoi);
		return new Schema(defin);
	}
	public static CubeMetadata readCubeMetaDataFile(String schemaName, String cubeName) throws UnsupportedEncodingException, ClassNotFoundException, IOException
	{
		String storePath = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION);
		String cubeMetadataFile = storePath + "/schemas/" + schemaName + "/" + cubeName + "/metadata";
		FileType fileType = FileFactory.getFileType(storePath);
		CubeMetadata cubeMeta = new CubeMetadata();
	    if (FileFactory.isFileExist(cubeMetadataFile, fileType)) 
	    {
		      //load metadata
	    	DataInputStream in = FileFactory.getDataInputStream(cubeMetadataFile, fileType);
	    	int schemaNameLen = in.readInt();
	        byte[] schemaNameBytes = new byte[schemaNameLen];
	        in.readFully(schemaNameBytes);
	        cubeMeta.setSchemaName(new String(schemaNameBytes, "UTF8"));
	        
	        int cubeNameLen = in.readInt();
	        byte[] cubeNameBytes = new byte[cubeNameLen];
	        in.readFully(cubeNameBytes);
	        cubeMeta.setCubeName(new String(cubeNameBytes, "UTF8"));

	        int dataPathLen = in.readInt();
	        byte[]  dataPathBytes = new byte[dataPathLen];
	        in.readFully(dataPathBytes);
	        cubeMeta.setDataPath(new String(dataPathBytes, "UTF8"));

	        int versionLength = in.readInt();
	        byte[]  versionBytes = new byte[versionLength];
	        in.readFully(versionBytes);

	        int schemaLen = in.readInt();
	        byte[] schemaBytes = new byte[schemaLen];
	        in.readFully(schemaBytes);
	        cubeMeta.setSchema(new String(schemaBytes, "UTF8"));

	        int partitionLength = in.readInt();
	        byte[] partitionBytes = new byte[partitionLength];
	        in.readFully(partitionBytes);
	        ByteArrayInputStream inStream = new ByteArrayInputStream(partitionBytes);
	        ObjectInputStream objStream = new ObjectInputStream(inStream);
	        cubeMeta.setPartitioner((Partitioner)objStream.readObject());
	        objStream.close();
   	        in.close();
	    }
	    return cubeMeta;
	}
}
