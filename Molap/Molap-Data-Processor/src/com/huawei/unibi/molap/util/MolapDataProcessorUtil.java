/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbTx7nQdiHv7zUYcWRiQwkVLlNRAgpjeDdBPErFvTkGc2xW+k+0bImXcWenKQW1AbhfmF
+hICym5gYcUaNWYTH7NAhdt44aIOMByLn2LjBUuGWIZisd5pnOmU3ThQx5buwg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepMeta;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.MeasureMetaDataModel;
import com.huawei.unibi.molap.datastorage.store.filesystem.HDFSMolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.LocalMolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.CountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DistinctCountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MaxAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MinAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumDistinctAggregator;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore;
import com.huawei.unibi.molap.exception.MolapDataProcessorException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.olap.MolapDef;
//import com.huawei.unibi.molap.olap.MolapDef.Cube;
//import com.huawei.unibi.molap.olap.MolapDef.Schema;
//import com.huawei.unibi.molap.schema.metadata.AggregateTable;
//import mondrian.rolap.CacheControlImpl;
//import mondrian.rolap.RolapCube;
//import mondrian.rolap.RolapUtil;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;

/**
 * 
 * Project Name NSE V3R7C00 Module Name : Molap Data Processor Author K00900841
 * Created Date :21-May-2013 6:42:29 PM FileName : MolapDataProcessorUtil.java
 * Class Description : All the utility methods are present in this class Version
 * 1.0
 */
public final class MolapDataProcessorUtil
{
    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapDataProcessorUtil.class.getName());
    
    private MolapDataProcessorUtil()
    {
    	
    }
    /**
     * This method will be used to write measure metadata (max, min ,decimal
     * length)file
     * 
     * Measure metadat file will be in below format <max value for each
     * measures><min value for each measures><decimal length of each measures>
     * 
     * @throws MolapDataProcessorException
     * @throws IOException
     * @throws IOException
     * 
     */
    private static void writeMeasureMetaDataToFileLocal(double[] maxValue,
            double[] minValue, int[] decimalLength, double[] uniqueValue,
            char[] aggType, byte[] dataTypeSelected,double[] minValueFact,String measureMetaDataFileLocation)
            throws MolapDataProcessorException
    {
        int length = maxValue.length;
        // calculating the total size of buffer, which is nothing but [(number
        // of measure * (8*2)) +(number of measure *4)]
        // 8 for holding the double value and 4 for holding the int value, 8*2
        // because of max and min value
        int totalSize = length * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE * 3 + length
                * MolapCommonConstants.INT_SIZE_IN_BYTE+length*MolapCommonConstants.CHAR_SIZE_IN_BYTE+length;
        if(minValueFact != null)
        {
        	totalSize += length * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);

        // add all the max
        for(int j = 0;j < maxValue.length;j++)
        {
            byteBuffer.putDouble(maxValue[j]);
        }

        // add all the min
        for(int j = 0;j < minValue.length;j++)
        {
            byteBuffer.putDouble(minValue[j]);
        }

        // add all the decimal
        for(int j = 0;j < decimalLength.length;j++)
        {
            byteBuffer.putInt(decimalLength[j]);
        }
        
        for(int j = 0;j < uniqueValue.length;j++)
        {
            byteBuffer.putDouble(uniqueValue[j]);
        }
        
        if(null != aggType)
        {
        	for(int j = 0;j < aggType.length;j++)
            {
                byteBuffer.putChar(aggType[j]);
            }
        }
        
        for(int j = 0;j < dataTypeSelected.length;j++)
        {
            byteBuffer.put(dataTypeSelected[j]);
        }
        
        if(minValueFact != null)
        {
            for(int j = 0;j < minValueFact.length;j++)
            {
                byteBuffer.putDouble(minValueFact[j]);
            }
        }
        
        // flip the buffer
        byteBuffer.flip();
        FileOutputStream stream = null;
        FileChannel channel = null;
        try
        {
            stream = new FileOutputStream(measureMetaDataFileLocation);
            // get the channel
            channel = stream.getChannel();
            // write the byte buffer to file
            channel.write(byteBuffer);
        }
        catch(IOException exception)
        {
            throw new MolapDataProcessorException("Problem while writing the measure meta data file", exception);
        }
        finally
        {
            MolapUtil.closeStreams(channel, stream);
        }
    }
    
    public static void writeMeasureMetaDataToFile(double[] maxValue,
            double[] minValue, int[] decimalLength, double[] uniqueValue,
            char[] aggType, byte[] dataTypeSelected,String measureMetaDataFileLocation)
            throws MolapDataProcessorException
    {
    	writeMeasureMetaDataToFileLocal(maxValue, minValue, decimalLength, uniqueValue, aggType, dataTypeSelected,null, measureMetaDataFileLocation);
    }
    
    public static void writeMeasureMetaDataToFileForAgg(double[] maxValue,
            double[] minValue, int[] decimalLength, double[] uniqueValue,
            char[] aggType, byte[] dataTypeSelected,double[] minValueAgg, String measureMetaDataFileLocation)
            throws MolapDataProcessorException
    {
    	writeMeasureMetaDataToFileLocal(maxValue, minValue, decimalLength, uniqueValue, aggType, dataTypeSelected, minValueAgg, measureMetaDataFileLocation);
    }
    
    /**
     * This method will be used to get the cube info list
     * 
     * @return all cube info
     * 
     * @throws MolapDataProcessorException
     *             problem while getting the schema path
     * 
     */
  /*  public static List<MolapCubeInfo> getAllMolapCubeInfo() throws MolapDataProcessorException
    {
        Set<String> allSchemaPath = null;
        List<String> tableName = null;
        Schema schema = null;
        Cube[] cubes = null;
        Cube cube = null;
        List<MolapCubeInfo> molapCubeInfoList = null;
        MolapCubeInfo cubeInfo = null;
        try
        {
            allSchemaPath = getAllSchemaPath();
            molapCubeInfoList = new ArrayList<MolapCubeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            for(String schemaPath : allSchemaPath)
            {
                schema = MolapSchemaParser.loadXML(schemaPath);
                cubes = MolapSchemaParser.getMondrianCubes(schema);
                for(int i = 0;i < cubes.length;i++)
                {
                    cube = cubes[i];
                    if(!MolapSchemaParser.validateCube(cube, schema,false))
                    {
                        continue;
                    }
                    tableName = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    String factTableName = MolapSchemaParser.getFactTableName(cube);
                    tableName.add(factTableName);
                    AggregateTable[] aggregateTable = MolapSchemaParser.getAggregateTable(cube, schema);
                    for(int j = 0;j < aggregateTable.length;j++)
                    {
                        tableName.add(aggregateTable[j].getAggregateTableName());
                    }
                    cubeInfo = new MolapCubeInfo();
                    cubeInfo.setCubeName(cube.name);
                    cubeInfo.setCube(cube);
                    cubeInfo.setSchemaName(schema.name);
                    cubeInfo.setTableNames(tableName);
                    cubeInfo.setSchemaPath(schemaPath);
                    molapCubeInfoList.add(cubeInfo);
                }
            }
        }
        catch(MolapDataProcessorException e)
        {
            throw new MolapDataProcessorException("Problem while getting the schema path", e);
        }
        return molapCubeInfoList;
    }
*/
    /**
     * This method will be used to read all the RS folders
     * 
     * @param schemaName
     * @param cubeName
     * 
     */
    public static File[] getAllRSFiles(String schemaName, String cubeName, String baseLocation)
    {
        /*String baseLocation = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);*/
        baseLocation = baseLocation + File.separator + schemaName + File.separator + cubeName;
        File file = new File(baseLocation);
        File[] rsFile = file.listFiles(new FileFilter()
        {

            @Override
            public boolean accept(File pathname)
            {
                return pathname.getName().startsWith(MolapCommonConstants.RESTRUCTRE_FOLDER);
            }
        });
        return rsFile;
    }
    public static File[] getAllRSFiles(String schemaName, String cubeName)
    {
        String tempLocationKey = schemaName+'_'+cubeName;
        String baseLocation = MolapProperties.getInstance().getProperty(tempLocationKey,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        baseLocation = baseLocation + File.separator + schemaName + File.separator + cubeName;
        File file = new File(baseLocation);
        File[] rsFile = file.listFiles(new FileFilter()
        {

            @Override
            public boolean accept(File pathname)
            {
                return pathname.getName().startsWith(MolapCommonConstants.RESTRUCTRE_FOLDER);
            }
        });
        return rsFile;
    }
    /**
     * This method will be used to read all the load folders
     * 
     * @param rsFiles
     * @param tableName
     * @return
     * 
     */
    public static File[] getAllLoadFolders(File rsFiles, String tableName)
    {
        File file = new File(rsFiles + File.separator + tableName);

        File[] listFiles = file.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return (pathname.isDirectory() && pathname.getName().startsWith(MolapCommonConstants.LOAD_FOLDER));
            }
        });
        return listFiles;
    }
    
    /**
     * This method will be used to read all the load folders
     * 
     * @param rsFiles
     * @param tableName
     * @return
     * 
     */
    public static File[] getAllLoadFoldersWithOutInProgressExtension(File rsFiles, String tableName)
    {
        File file = new File(rsFiles + File.separator + tableName);

        File[] listFiles = file.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return (pathname.isDirectory()
                        && pathname.getName().startsWith(
                                MolapCommonConstants.LOAD_FOLDER) && !pathname
                        .getName().contains(
                                MolapCommonConstants.FILE_INPROGRESS_STATUS));
            }
        });
        return listFiles;
    }
    
    /**
     * This method will be used to read all the load folders
     * 
     * @param rsFiles
     * @param tableName
     * @return
     * 
     */
    public static File[] getAllLoadFolders(File tableFolder)
    {
        //File file = new File(rsFiles + File.separator + tableName);

        File[] listFiles = tableFolder.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return (pathname.isDirectory() && pathname.getName().startsWith(MolapCommonConstants.LOAD_FOLDER));
            }
        });
        return listFiles;
    }

    
    /**
     * This method will be used to read all the load folders
     * 
     * @param rsFiles
     * @param tableName
     * @return
     * 
     */
    public static File[] getChildrenFolders(File parentFolder)
    {
        //File file = new File(rsFiles + File.separator + tableName);

        File[] listFiles = parentFolder.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return pathname.isDirectory();
            }
        });
        return listFiles;
    }
    
    
    /**
     * This method will be used to read all the fact files
     * 
     * @param sliceLocation
     * @param tableName
     * @return
     * 
     */
    public static File[] getAllFactFiles(File sliceLocation, final String tableName)
    {
        File file = new File(sliceLocation.getAbsolutePath());

        File[] listFiles = file.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return pathname.getName().startsWith(tableName)
                        && pathname.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT);
            }
        });
        return listFiles;
    }

    /**
     * This method will be used to read all the files excluding fact files.
     * 
     * @param sliceLocation
     * @param tableName
     * @return
     * 
     */
    public static File[] getAllFilesExcludeFact(String sliceLocation, final String tableName)
    {
        File file = new File(sliceLocation);

        File[] listFiles = file.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return (!(pathname.getName().startsWith(tableName)));
            }
        });
        return listFiles;
    }

    /**
     * This method will be used to read all the files which are retainable as
     * per the policy applied
     * 
     * @param sliceLocation
     * @param tableName
     * @return
     * 
     */
    public static File[] getAllRetainableFiles(String sliceLocation, final String tableName
           )
    {
        File file = new File(sliceLocation + File.separator + tableName);

        File[] listFiles = file.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return !pathname.getName().startsWith(tableName);
            }
        });
        return listFiles;
    }

    /**
     * This method will be used to for sending the new slice signal to engine
     * 
     * @throws MolapDataProcessorException
     * 
     * @throws KettleException
     *             if any problem while informing the engine
     * 
     */
    public static void sendLoadSignalToEngine(String storeLocation) throws MolapDataProcessorException
    {
    	if(!Boolean.parseBoolean(MolapProperties.getInstance().getProperty("send.signal.load", "true")))
    	{
    		return;
    	}
        try
        {
            // inform engine to load new slice
            Class<?> c = Class.forName("com.huawei.unibi.molap.engine.datastorage.CubeSliceLoader");
            Class[] argTypes = new Class[]{};
            // get the instance of CubeSliceLoader
            Method main = c.getDeclaredMethod("getInstance", argTypes);
            Object invoke = main.invoke(null, null);
            Class[] argTypes1 = new Class[]{String.class};

            // ionvoke loadSliceFromFile
            Method declaredMethod = c.getDeclaredMethod("loadSliceFromFiles", argTypes1);
            // pass cube name and store location
            String[] a = {storeLocation};
            declaredMethod.invoke(invoke, a);
        }
        catch(ClassNotFoundException classNotFoundException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", classNotFoundException);
        }
        catch(NoSuchMethodException noSuchMethodException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", noSuchMethodException);
        }
        catch(IllegalAccessException illegalAccessException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", illegalAccessException);
        }
        catch(InvocationTargetException invocationTargetException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", invocationTargetException);
        }
    }

    /**
     * @param storeLocation
     * @throws MolapDataProcessorException
     */
    public static void sendUpdateSignaltoEngine(String storeLocation) throws MolapDataProcessorException
    {
    	if(!Boolean.parseBoolean(MolapProperties.getInstance().getProperty("send.signal.load", "true")))
    	{
    		return;
    	}
        try
        {
            // inform engine to load new slice
            Class<?> sliceLoaderClass = Class.forName("com.huawei.unibi.molap.engine.datastorage.CubeSliceLoader");
            Class[] argTypes = new Class[]{};
            // get the instance of CubeSliceLoader
            Method instanceMethod = sliceLoaderClass.getDeclaredMethod("getInstance", argTypes);
            Object invoke = instanceMethod.invoke(null, null);
            Class[] argTypes1 = new Class[]{String.class};

            // ionvoke loadSliceFromFile
            Method updateMethod = sliceLoaderClass.getDeclaredMethod("updateSchemaHierarchy", argTypes1);
            // pass cube name and store location
            String[] a = {storeLocation};
            updateMethod.invoke(invoke, a);
        }
        catch(ClassNotFoundException classNotFoundException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", classNotFoundException);
        }
        catch(NoSuchMethodException noSuchMethodException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", noSuchMethodException);
        }
        catch(IllegalAccessException illegalAccessException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", illegalAccessException);
        }
        catch(InvocationTargetException invocationTargetException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", invocationTargetException);
        }
    }
    /**
     * This method will be used to for sending the new slice signal to engine
     * 
     * @throws MolapDataProcessorException
     * 
     * @throws KettleException
     *             if any problem while informing the engine
     * 
     */
    public static void sendDeleteSignalToEngine(String[] storeLocation) throws MolapDataProcessorException
    {
    	if(!Boolean.parseBoolean(MolapProperties.getInstance().getProperty("send.signal.load", "true")))
    	{
    		return;
    	}
        try
        {
            // inform engine to load new slice
            Class<?> c = Class.forName("com.huawei.unibi.molap.engine.datastorage.CubeSliceLoader");
            Class[] argTypes = new Class[]{};
            // get the instance of CubeSliceLoader
            Method main = c.getDeclaredMethod("getInstance", argTypes);
            Object invoke = main.invoke(null,null);
            Class[] argTypes1 = new Class[]{String[].class};

            // ionvoke loadSliceFromFile
            Method declaredMethod = c.getDeclaredMethod("deleteSlices", argTypes1);
            Object[] objectStoreLocation={storeLocation};
            declaredMethod.invoke(invoke, objectStoreLocation);
        }
        catch(ClassNotFoundException classNotFoundException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", classNotFoundException);
        }
        catch(NoSuchMethodException noSuchMethodException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", noSuchMethodException);
        }
        catch(IllegalAccessException illegalAccessException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", illegalAccessException);
        }
        catch(InvocationTargetException invocationTargetException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", invocationTargetException);
        }
    }
    
    /**
     * 
     * @Author M00903915
     * @Description : clearCubeCache
     * @param schemaName
     * @param cubeName
     */
    public static boolean clearCubeCache(String schemaName, String cubeName) 
    {
        try
        {
            Class<?> c = Class.forName("mondrian.rolap.CacheControlImpl");
            // get the instance of CubeSliceLoader
            Object newInstance = c.newInstance();
            Class<?> argTypes1 = String.class;
            Class<?> argTypes2 = String.class;
            Method declaredMethod =newInstance.getClass().getMethod("flushCubeCache", argTypes1,argTypes2);
            Object value=declaredMethod.invoke(newInstance, schemaName, cubeName);
            return ((Boolean)value).booleanValue();
        }
        catch(ClassNotFoundException classNotFoundException)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache " + classNotFoundException);
        }
        catch(NoSuchMethodException noSuchMethodException)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache " + noSuchMethodException);
        }
        catch(IllegalAccessException illegalAccessException)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache " + illegalAccessException);
        }
        catch(InvocationTargetException invocationTargetException)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache "
                            + invocationTargetException);
        }
        catch(InstantiationException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache "
                            + e);
        }
        return false;
    }
    
//    /**
//     * 
//     * @Author M00903915
//     * @Description : clearCubeCache
//     * @param schemaName
//     * @param cubeName
//     */
//    public static void flushSchemaCache() 
//    {
//        try
//        {
//            CacheControlImpl impl = new CacheControlImpl();
//            impl.flushSchemaCache();
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Problem while flushing the cache");
//        }
//    }
    
    /**
     * This method will be used to for sending the new slice signal to engine
     * 
     * @throws KettleException
     *             if any problem while informing the engine
     * 
     */
    public static void sendUpdateSignalToEngine(String newSliceLocation, String[] oldSliceLocation, boolean isUpdateMemberCall)
            throws MolapDataProcessorException
    {
    	if(!Boolean.parseBoolean(MolapProperties.getInstance().getProperty("send.signal.load", "true")))
    	{
    		return;
    	}
        try
        {
            // inform engine to load new slice
            Class<?> c = Class.forName("com.huawei.unibi.molap.engine.datastorage.CubeSliceLoader");
            Class[] argTypes = new Class[]{};
            // get the instance of CubeSliceLoader
            Method main = c.getDeclaredMethod("getInstance", argTypes);
            Object invoke = main.invoke(null, null);
            Class[] argTypes1 = new Class[]{String.class, String[].class ,Boolean.TYPE, Boolean.TYPE};

            // ionvoke loadSliceFromFile
            Method declaredMethod = c.getDeclaredMethod("updateSlices", argTypes1);
            declaredMethod.invoke(invoke, newSliceLocation, oldSliceLocation,true,isUpdateMemberCall);
        }
        catch(ClassNotFoundException classNotFoundException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", classNotFoundException);
        }
        catch(NoSuchMethodException noSuchMethodException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", noSuchMethodException);
        }
        catch(IllegalAccessException illegalAccessException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", illegalAccessException);
        }
        catch(InvocationTargetException invocationTargetException)
        {
            throw new MolapDataProcessorException("Problem While informing BI Server", invocationTargetException);
        }
    }

    /**
     * This method will be used to get the all schema path present in unibi
     * server
     * 
     * @return schema path list
     * 
     */
  /*  private static Set<String> getAllSchemaPath() throws MolapDataProcessorException
    {

//        IPentahoSession pSession = PentahoSystem.get(IPentahoSession.class, "systemStartupSession", null);
//        MondrianCatalogHelper helper = (MondrianCatalogHelper)PentahoSystem.get(IMondrianCatalogService.class,
//                "IMondrianCatalogService", null);
//        final int MOLAP_URL_START_LENGNTH = "molap://".length();
//        List<MondrianCatalog> list = helper.getCatalogList(pSession);
//        ListIterator<MondrianCatalog> itr = list.listIterator();
//        MondrianCatalog catalog = null;
//        String schemaFilePath = "";
//        String url = "";
//        Set<String> schemaFilePathList = new HashSet<String>();
//        while(itr.hasNext())
//        {
//            catalog = itr.next();
//
//            List<MondrianCube> cubes = catalog.getSchema().getCubes();
//            for(int i = 0;i < cubes.size();i++)
//            {
//                String datasourceName = getDataSourceName(catalog);
//                IDatasource datasource = getDataSource(datasourceName);
//                if(null == datasource)
//                {
//                    //throw new MolapDataProcessorException("Problem while getting the data source, datasource is null");
//                    continue;
//                }
//                url = datasource.getUrl();
//                if(url.startsWith("molap://"))
//                {
//                    url = url.substring(MOLAP_URL_START_LENGNTH);
//
//                    String[] dsStrParams = url.split(";");
//                    for(String dsStrParam : dsStrParams)
//                    {
//                        if(dsStrParam.startsWith("sourceDB"))
//                        {
//                            url = dsStrParam.split("=")[1];
//                        }
//                    }
//                    schemaFilePath = catalog.getDefinition().replace("solution:/", "");
//                    
//                    IDatasource iDatasource = getDataSource(url);
//                    if(null == iDatasource)
//                    {
//                        LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                                "Not Able to get datasource: "+datasource);
//                    }
//                    schemaFilePathList.add(PentahoSystem.getApplicationContext().getSolutionPath("") + schemaFilePath);
//                }
//            }
//        }
        return null;
    }*/
    
//    public static void loadAllSchema()
//    {
//        IPentahoSession pSession = PentahoSystem.get(IPentahoSession.class,
//                "systemStartupSession", null);
//        MondrianCatalogHelper helper = (MondrianCatalogHelper)PentahoSystem
//                .get(IMondrianCatalogService.class, "IMondrianCatalogService",
//                        null);
//        List<MondrianCatalog> list = helper.getCatalogList(pSession);
//        ListIterator<MondrianCatalog> itr = list.listIterator();
//        try
//        {
//            while(itr.hasNext())
//            {
//                RolapUtil.loadSchemaToCache(itr.next().getName());
//            }
//        }
//        catch(ClassNotFoundException e)
//        {
//           LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Problem while loading the schema");
//        }
//        catch(NoSuchMethodException e)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Problem while loading the schema");
//        }
//        catch(IllegalAccessException e)
//        {
//             LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Problem while loading the schema");
//        }
//        catch(InvocationTargetException e)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Problem while loading the schema");
//        }
//
//    }

//    /**
//     * 
//     * @param catalog
//     * @return
//     * 
//     */
//    private static String getDataSourceName(MondrianCatalog catalog)
//    {
//        String[] ds = catalog.getDataSourceInfo().split(";");
//        String datasource = null;
//        for(int i = 0;i < ds.length;i++)
//        {
//            String[] con = ds[i].split("=");
//            if("DataSource".equals(con[0]))
//            {
//                datasource = con[1];
//                break;
//            }
//        }
//        return datasource;
//    }
//
//    private static IDatasource getDataSource(String datasource) throws MolapDataProcessorException
//    {
//        IDatasourceMgmtService datasourceMgmtSvc;
//        IDatasource iDataSource = null;
//        try
//        {
//            datasourceMgmtSvc = (IDatasourceMgmtService)PentahoSystem.getObjectFactory().get(
//                    IDatasourceMgmtService.class, null);
//            iDataSource = datasourceMgmtSvc.getDatasource(datasource);
//        }
//        catch(Exception e)
//        {
//            throw new MolapDataProcessorException("Problem while getting Data source", e);
//        }
//        return iDataSource;
//    }

    /**
     * 
     * @param fileToBeDeleted
     */
    public static String recordFilesNeedsToDeleted(Set<String> fileToBeDeleted)
    {
        Iterator<String> itr = fileToBeDeleted.iterator();
        String filenames = null;
        BufferedWriter writer = null;
        try
        {
            while(itr.hasNext())
            {
                String filenamesToBeDeleted = itr.next();

                if(null == filenames)
                {
                    filenames = filenamesToBeDeleted.substring(0, filenamesToBeDeleted.lastIndexOf(File.separator));
                    filenames = filenames +File.separator+ MolapCommonConstants.RETENTION_RECORD;
                    File file = new File(filenames);
                    
                    if(!file.exists())
                    {
                       if(!file.createNewFile())
                       {
                    	   throw new Exception("Unable to create file "+file.getName());
                       }
                    }
                }
                if(null == writer)
                {
                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filenames),"UTF-8"));
                }

                writer.write(filenamesToBeDeleted);
                writer.newLine();

            }
        }
        catch(Exception e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "recordFilesNeedsToDeleted");
        }

        finally
        {
            MolapUtil.closeStreams(writer);
        }
        return filenames;
    }

//    /**
//     * getAllAggTables
//     * @param rolapCube
//     * @return AggTable[]
//     */
//    //public static AggTable[] getAllAggTables(RolapCube rolapCube)
//    public static AggTable[] getAllAggTables(RolapCube rolapCube) throws MolapDataProcessorException
//    {
//
//        MondrianDef.AggTable[] aggtables = null;
//        try
//        {
//
//            List<MolapCubeInfo> listMolapCubeInfo = getAllMolapCubeInfo();
//            Cube cube;
//            Iterator<MolapCubeInfo> itr = listMolapCubeInfo.iterator();
//            while(itr.hasNext())
//            {
//                MolapCubeInfo molapCubeInfo = itr.next();
//                if(rolapCube.getName().equals(molapCubeInfo.getCubeName()))
//                {
//
//                    cube = molapCubeInfo.getCube();
//                    aggtables = ((MondrianDef.Table)cube.fact).aggTables;
//                    return aggtables;
//
//                }
//            }
//
//        }
//        catch(MolapDataProcessorException e)
//        {
//
//                throw new MolapDataProcessorException("Problem while getting aggregate table metadata for data retention", e);
//            
//        }
//        return aggtables;
//    }

//    /**
//     * readMemberFileAndPopulateSurrogateKey
//     * @param folderList
//     * @param surrogateKeys
//     * @param levelName
//     * @param dataPeriod void
//     */
//    public static void readMemberFileAndPopulateSurrogateKey(
//            List<File> folderList, Map<String, Integer> surrogateKeys,
//            String levelName, DataRetentionPeriodMemberIntf dataPeriod)
//    {
//        FileInputStream fos = null;
//        FileChannel fileChannel= null;
//        try
//        {
//            for(File file : folderList)
//            {
//                fos = new FileInputStream(file);
//                fileChannel = fos.getChannel();
//                long size = fileChannel.size();
//                while(fileChannel.position() < size)
//                {
//                    ByteBuffer rowlengthToRead = ByteBuffer.allocate(4);
//                    fileChannel.read(rowlengthToRead);
//                    rowlengthToRead.rewind();
//                    int len = rowlengthToRead.getInt();
//                    ByteBuffer row = ByteBuffer.allocate(len);
//                    fileChannel.read(row);
//                    row.rewind();
//                    int toread = row.getInt();
//                    byte[] bb = new byte[toread];
//                    row.get(bb);
//                    String value=null;//CHECKSTYLE:OFF    Approval No:Approval-245
//                    try
//                    {//CHECKSTYLE:ON
//                        value = new String(Base64.decodeBase64(bb),"UTF-8");
//                    }
//                    catch(Exception e)
//                    {
//                        LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
//                                "Unable to decrypt value for file " + file.getName());
//                        value = new String(Base64.decodeBase64(bb));
//                    }
//                    int surrogateValue = row.getInt();
//                    if(dataPeriod instanceof DataRetentionPeriodMember
//                            && value.equals(((DataRetentionPeriodMember)dataPeriod).getMemberNames()))
//                    {
//                        surrogateKeys.put(levelName, surrogateValue);
//                    }
//                    else if(dataPeriod instanceof DataRetentionDayMember
//                            && value.equals(((DataRetentionDayMember)dataPeriod).getMemberNames()))
//                    {
//                    	
//                        surrogateKeys.put(levelName, surrogateValue);
//                    }
//                }
//            }
//        }
//        catch (IOException e) 
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
//                    "Unable to read file " + levelName);
//        }
//        finally
//        {
//            MolapUtil.closeStreams(fileChannel, fos);
//        }
//    }
    
    /**
     * Pass the folder name, The API will tell you whether the
     * retention processing is in progress or not. Restructure and
     * Merging can call this API inorder to continue with their process.
     * @param folderName
     * @return boolean.
     */
    public static boolean isRetentionProcessIsInProgress(String folderName)
    {
        boolean inProgress = false;
        String deletionRecordFilePath = folderName + File.separator
                + MolapCommonConstants.RETENTION_RECORD;
        File deletionRecordFileName = new File(deletionRecordFilePath);
        if(deletionRecordFileName.exists())
        {
            inProgress = true;
        }

        return inProgress;

    }
    
    /**
     * 
     * @param folderName
     * @throws MolapDataProcessorException
     */
    public static void deleteFileAsPerRetentionFileRecord(String folderName) throws MolapDataProcessorException 
    {
        String deletionRecordFilePath = folderName + File.separator
                + MolapCommonConstants.RETENTION_RECORD;

        File fileTobeDeleted = null;
        BufferedReader reader = null;
        try
        {
            reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(deletionRecordFilePath) , "UTF-8"));
            String sCurrentLine;
            while((sCurrentLine = reader.readLine()) != null)
            {
                fileTobeDeleted = new File(sCurrentLine);
                if(fileTobeDeleted.exists())
                {
                    if(!fileTobeDeleted.delete())
                    {
						LOGGER.debug(
								MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
								"Could not delete the file : "
										+ fileTobeDeleted
												.getAbsolutePath());
                    }
                }

            }

        }
        catch(FileNotFoundException e)
        {
            
                // TODO Auto-generated catch block
                throw new MolapDataProcessorException(
                        "Data Deletion is Failed...");
        }
        catch(IOException e)
        {
            // TODO Auto-generated catch block
            throw new MolapDataProcessorException(
                    "Data Deletion is Failed...");
        }
        finally
        {
        	MolapUtil.closeStreams(reader);
        }

    }
    
//    /**
//     * This Method will process the Load folder by copying all the files
//     * 
//     * @param folderName
//     * @param listOfAllMetadataFiles
//     * @param fileToBeUpdated 
//     * @throws MolapDataProcessorException  
//     * @throws DataRetentionException 
//     *///CHECKSTYLE:OFF    Approval No:Approval-279
//    public static void processLoadFolderWithModifiedData(String folderName,
//            File[] listOfAllMetadataFiles, String fileToBeUpdated) throws MolapDataProcessorException, DataRetentionException 
//    {//CHECKSTYLE:ON
//        File inProgressFile = new File(folderName
//                + MolapCommonConstants.FILE_INPROGRESS_STATUS);
//        boolean isDirCreated;
//        if(!inProgressFile.exists())
//        {
//             isDirCreated = new File(folderName
//                    + MolapCommonConstants.FILE_INPROGRESS_STATUS).mkdirs();
//        }
//        else
//        {
//            isDirCreated=true;
//        }
//
//        if(isDirCreated)
//        {
////            for(int i = 0;i < listOfAllMetadataFiles.length;i++)
////            {
////                File file = listOfAllMetadataFiles[i];
////
////                String destFileName = inProgressFile.getAbsolutePath()
////                        + File.separator + file.getName();
//                try
//                {
//                    for(int i = 0;i < listOfAllMetadataFiles.length;i++)
//                    {
//                        File file = listOfAllMetadataFiles[i];
//
//                        String destFileName = inProgressFile.getAbsolutePath()
//                                + File.separator + file.getName();
//                            MolapUtil.copySchemaFile(file.getAbsolutePath(),
//                                    destFileName);
//                    }
//                }
//                catch(MolapUtilException e)
//                {
//                    // TODO Auto-generated catch block
//                    throw new MolapDataProcessorException(
//                            "Data Deletion is Failed...");
//                }
////            }
//            File originalFileName = new File(folderName);
//            try
//            {
//                MolapUtil.deleteFoldersAndFiles(folderName);
//            }
//            catch(MolapUtilException e)
//            {
//                throw new DataRetentionException(
//                        "Data Retention operation has been failed", e);
//            }
//            boolean status = inProgressFile.renameTo(originalFileName);
//            if(status)
//            {
//                String deletionRecordFilePath = folderName + File.separator
//                        + MolapCommonConstants.RETENTION_RECORD;
//                File deletionRecordFileName = new File(deletionRecordFilePath);
//                if(deletionRecordFileName.exists())
//                {
//                    if(!deletionRecordFileName.delete())
//                    {
//						LOGGER.debug(
//								MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//								"Could not delete the file : "
//										+ deletionRecordFileName
//												.getAbsolutePath());
//                    }
//                }
//            }
//
//        }
//        else
//        {
//            throw new DataRetentionException(
//                    "Data Retention operation has been failed");
//        }
//    }
   
    /**
     * Below method will be used to write slice metadata
     * 
     * @param sliceMetaData
     *          slice meta data
     * @param sliceMetadatFilePath
     *          sliceMetadatFilePath
     * @throws MolapDataProcessorException 
     *
     */
//    public static void writeSliceMetaDataFile(SliceMetaData sliceMetaData, String sliceMetadatFilePath) throws MolapDataProcessorException
//    {
//        FileOutputStream fileOutputStream = null;
//        ObjectOutputStream objectOutputStream = null;
//        try
//        {
//            fileOutputStream = new FileOutputStream(sliceMetadatFilePath + File.separator
//                    + MolapCommonConstants.SLICE_METADATA_FILENAME);
//            objectOutputStream = new ObjectOutputStream(fileOutputStream);
//            objectOutputStream.writeObject(sliceMetaData);
//        }
//        catch(FileNotFoundException e)
//        {
//            throw new MolapDataProcessorException("Problem while writing the slice metadata file", e);
//        }
//        catch(IOException e)
//        {
//            throw new MolapDataProcessorException("Problem while writing the slice metadata file", e);
//        }
//        finally
//        {
//            MolapUtil.closeStreams(objectOutputStream, fileOutputStream);
//        }
//    }
    
    /**
     * This mehtod will be used to copmare to byte array
     * 
     * @param b1
     *          b1
     * @param b2
     *          b2
     * @return compare result 
     *
     */
    public static int compare(byte[] b1, byte[] b2)
    {
        int cmp = 0;
        int length = b1.length;
        for(int i = 0;i < length;i++)
        {
            int a = (b1[i] & 0xff);
            int b = (b2[i] & 0xff);
            cmp = a - b;
            if(cmp != 0)
            {
                cmp = cmp < 0 ? -1 : 1;
                break;
            }
        }
        return cmp;
    }
    
    /**
     * Below method will be used to get the buffer size 
     * @param numberOfFiles
     * @return buffer size 
     */
    public static int getFileBufferSize(int numberOfFiles, MolapProperties instance, int deafultvalue)
    {
        int configuredBufferSize = 0;
        try
        {
            configuredBufferSize = Integer.parseInt(instance
                    .getProperty(MolapCommonConstants.SORT_FILE_BUFFER_SIZE));
        }
        catch(NumberFormatException e)
        {
            configuredBufferSize = deafultvalue;
        }
        int fileBufferSize = (configuredBufferSize*
                 MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR)
                / numberOfFiles;
        if(fileBufferSize < MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR)
        {
            fileBufferSize = MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
        }
        return fileBufferSize;
    }
    
    /**
     * 
     * Utility method to get the level cardinality  
     * @param dimensions
     *          dimension string with its cardianlity
     * @return cardinality array
     *
     */
    public static int[] getDimLens(String cardinalityString)
    {
        String[] dims = cardinalityString.split(MolapCommonConstants.COMA_SPC_CHARACTER);
        int[] dimLens = new int[dims.length];
        for(int i = 0;i < dims.length;i++)
        {
            dimLens[i] = Integer.parseInt(dims[i]);
        }

        return dimLens;
    }
    
    /**
     * Utility method to get level cardinality string 
     * @param dimCardinalities
     * @param aggDims
     * @return level cardinality string
     */
    public static String getLevelCardinalitiesString(
            Map<String, String> dimCardinalities, String[] aggDims)
    {
        StringBuilder sb = new StringBuilder();

        for(int i = 0;i < aggDims.length;i++)
        {
            String string = dimCardinalities.get(aggDims[i]);
            if(string != null)
            {
                sb.append(string);
                sb.append(MolapCommonConstants.COMA_SPC_CHARACTER);
            }
        }
        String resultStr = sb.toString();
        if(resultStr.endsWith(MolapCommonConstants.COMA_SPC_CHARACTER))
        {
            resultStr = resultStr.substring(0, resultStr.length()
                    - MolapCommonConstants.COMA_SPC_CHARACTER.length());
        }
        return resultStr;
    }
    
    
    /**
     * Utility method to get level cardinality string 
     * @param dimCardinalities
     * @param aggDims
     * @return level cardinality string
     */
    public static String getLevelCardinalitiesString(int[] dimlens)
    {
        StringBuilder sb = new StringBuilder();

        for(int i = 0;i < dimlens.length-1;i++)
        {
        	sb.append(dimlens[i]);
            sb.append(MolapCommonConstants.COMA_SPC_CHARACTER);
        }
        // in case where there is no dims present but high card dims are present.
        if(dimlens.length > 0)
        {
            sb.append(dimlens[dimlens.length-1]);
        }
        return sb.toString();
    }
    
    /**
     * getUpdatedAggregator
     * @param aggregator
     * @return String[]
     */
    public static String[] getUpdatedAggregator(String[] aggregator)
    {
        for(int i = 0;i < aggregator.length;i++)
        {
            if(MolapCommonConstants.COUNT.equals(aggregator[i]))
            {
                aggregator[i] = MolapCommonConstants.SUM;
            }
        }
        return aggregator;
    }
    
    /**
     * Below method will be used to get the all the slices loaded in the memory
     * if there is no slice present then this method will load the slice first
     * and return all the slice which will be used to for reading
     * 
     * @param cubeUniqueName
     * @param tableName
     * @param schemaPath
     * @return List<InMemoryCube>
     */
    public static List<InMemoryCube> getAllLoadedSlices(String cubeUniqueName, String tableName)
    {
        List<InMemoryCube> activeSlices = InMemoryCubeStore.getInstance().getActiveSlices(cubeUniqueName);
        List<InMemoryCube> requiredSlices = new ArrayList<InMemoryCube>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        InMemoryCube inMemoryCube = null;
        for(int i = 0;i < activeSlices.size();i++)
        {
            inMemoryCube = activeSlices.get(i);
            if(null!=inMemoryCube.getDataCache(tableName))
            {
                requiredSlices.add(inMemoryCube);
            }
        }
        return requiredSlices;
    }
    
    /**
     * getMaskedByte
     * @param queryDimensions
     * @param generator
     * @return
     */
    public static int[] getMaskedByte(int [] factLevelIndex, KeyGenerator generator)
    {

        Set<Integer> integers = new TreeSet<Integer>();
        //
        for(int i = 0;i < factLevelIndex.length;i++)
        {
			// in case of high card this will be -1
            if(factLevelIndex[i] == -1)
            {
                continue;
            }
            int[] range = generator.getKeyByteOffsets(factLevelIndex[i]);
            for(int j = range[0];j <= range[1];j++)
            {
                integers.add(j);
            }

        }
        //
        int[] byteIndexs = new int[integers.size()];
        int j = 0;
        for(Iterator<Integer> iterator = integers.iterator();iterator.hasNext();)
        {
            Integer integer = (Integer)iterator.next();
            byteIndexs[j++] = integer.intValue();
        }

        return byteIndexs;
    }
    
    
    public static MeasureMetaDataModel getMeasureModelForManual(String storeLocation,String tableName,int measureCount, FileType fileType)
	{
		MolapFile[] sortedPathForFiles = null;
		MeasureMetaDataModel model = null;
		sortedPathForFiles = MolapUtil.getAllFactFiles(storeLocation, tableName, fileType);
		if (null != sortedPathForFiles && sortedPathForFiles.length > 0)
		{

			model = ValueCompressionUtil.readMeasureMetaDataFile(
					storeLocation + File.separator
							+ MolapCommonConstants.MEASURE_METADATA_FILE_NAME
							+ tableName
							+ MolapCommonConstants.MEASUREMETADATA_FILE_EXT,
					measureCount);
		}
		return model;
	}
    
    
//    /**
//     * Below method will be used to get the fact file present in slice 
//     * 
//     * @param sliceLocation
//     *          slice location
//     * @return fact files array
//     *
//     */
//    private static MolapFile[] getAllFactFiles(String sliceLocation, final String tableName, FileType fileType)
//    {
//        MolapFile file = FileFactory.getMolapFile(sliceLocation, fileType);
//        MolapFile[] files = null;
//        if(file.isDirectory())
//        {
//            files = file.listFiles(new MolapFileFilter()
//            {
//                public boolean accept(MolapFile pathname)
//                {
//                    return ((!pathname.isDirectory())
//                            && (pathname.getName().startsWith(tableName)) && pathname
//                            .getName().endsWith(
//                                    MolapCommonConstants.FACT_FILE_EXT));
//
//                }
//            });
//        }
//        return files;
//    }
    
    public static double[] updateMergedMinValue(String schemaName, String cubeName, String tableName, int measureCount, String extension, int currentRestructNumber)
    {
        // get the table name
        String inputStoreLocation =schemaName + File.separator
                + cubeName;
        // get the base store location
        String tempLocationKey = schemaName+'_'+cubeName;
        String baseStorelocation = MolapProperties.getInstance().getProperty(
                tempLocationKey,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
                + File.separator + inputStoreLocation;
        int restructFolderNumber = currentRestructNumber/*MolapUtil
                .checkAndReturnNextRestructFolderNumber(baseStorelocation,"RS_")*/;
        if(restructFolderNumber<0)
        {
            return null;
        }
        baseStorelocation = baseStorelocation + File.separator
                + MolapCommonConstants.RESTRUCTRE_FOLDER + restructFolderNumber
                + File.separator + tableName;
        
        // get the current folder sequence
        int counter = MolapUtil
                .checkAndReturnCurrentLoadFolderNumber(baseStorelocation);
        if(counter<0)
        {
            return null;
        }
        File file = new File(baseStorelocation);
        // get the store location
        String storeLocation = file.getAbsolutePath() + File.separator
                + MolapCommonConstants.LOAD_FOLDER + counter+extension;
        
        String metaDataFileName = MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                + tableName + MolapCommonConstants.MEASUREMETADATA_FILE_EXT;
        String measureMetaDataFileLocation = storeLocation
                + metaDataFileName;
		double[] mergedMinValue = ValueCompressionUtil.readMeasureMetaDataFile(
				measureMetaDataFileLocation, measureCount).getMinValue();
		return mergedMinValue;
    }
    
	/**
	 * @param storeLocation
	 */
	public static void renameBadRecordsFromInProgressToNormal(
			String storeLocation) {
		// get the base store location
		String badLogStoreLocation = MolapProperties.getInstance().getProperty(
				MolapCommonConstants.MOLAP_BADRECORDS_LOC);
		badLogStoreLocation = badLogStoreLocation + File.separator
				+ storeLocation;

		// File badRecordFolder = new File(badLogStoreLocation);

		// if(!badRecordFolder.exists())
		// {
		// return;
		// }
		FileType fileType = FileFactory.getFileType(badLogStoreLocation);
		try {
			if (!FileFactory.isFileExist(badLogStoreLocation, fileType)) {
				return;
			}
		} catch (IOException e1) {
			LOGGER.info(
					MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
					"bad record folder does not exist");
		}
		MolapFile molapFile = null;
		if (fileType.equals(FileFactory.FileType.HDFS)) {
			molapFile = new HDFSMolapFile(badLogStoreLocation);
		} else {
			molapFile = new LocalMolapFile(badLogStoreLocation);
		}

		MolapFile[] listFiles = molapFile.listFiles(new MolapFileFilter() {
			@Override
			public boolean accept(MolapFile pathname) {
				if (pathname.getName().indexOf(
						MolapCommonConstants.FILE_INPROGRESS_STATUS) > -1) {
					return true;
				}
				return false;
			}
		});

		// File[] listFiles = badRecordFolder.listFiles(new FileFilter()
		// {
		//
		// @Override
		// public boolean accept(File pathname)
		// {
		// if(pathname.getName().indexOf(MolapCommonConstants.FILE_INPROGRESS_STATUS)
		// > -1)
		// {
		// return true;
		// }
		// return false;
		// }
		// });
		String badRecordsInProgressFileName = null;
		String changedFileName = null;
//		String badRecordEncryption = MolapProperties.getInstance().getProperty(MolapCommonConstants.MOLAP_BADRECORDS_ENCRYPTION);
		// CHECKSTYLE:OFF Approval No:Approval-367
		for (MolapFile badFiles : listFiles) {
			// CHECKSTYLE:ON
			badRecordsInProgressFileName = badFiles.getName();

			changedFileName = badLogStoreLocation + File.separator + badRecordsInProgressFileName.substring(0,
					badRecordsInProgressFileName.lastIndexOf('.'));
			
			badFiles.renameTo(changedFileName);

			if (badFiles.exists()) {
				if (!badFiles.delete()) {
					LOGGER.error(
							MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
							"Unable to delete File : " + badFiles.getName());
				}
			}
		}// CHECKSTYLE:ON
	}
	
	/**
	 * @param sliceMetaDataFilePath
	 * @param sliceMetaData
	 * @throws KettleException
	 */
	public static void writeFileAsObjectStream(String sliceMetaDataFilePath,
			SliceMetaData sliceMetaData) throws KettleException {
		FileOutputStream fileOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        try
        {
            fileOutputStream = new FileOutputStream(sliceMetaDataFilePath);
            objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(sliceMetaData);
        }
        catch(FileNotFoundException e)
        {
            throw new KettleException("slice metadata file not found", e);
        }
        catch(IOException e)
        {
            throw new KettleException("Not able to write slice metadata File",
                    e);
        }
        finally
        {
            MolapUtil.closeStreams(objectOutputStream, fileOutputStream);
        }
	}
    
	/**
	 * @param remarks
	 * @param stepMeta
	 * @param input
	 */
	public static void checkResult(List<CheckResultInterface> remarks,
			StepMeta stepMeta, String[] input) {
		CheckResult cr;

        // See if we have input streams leading to this step!
        if(input.length > 0)
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta);
            remarks.add(cr);
        }
        else
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);
            remarks.add(cr);
        }
	}
	
	/**
	 * @param remarks
	 * @param stepMeta
	 * @param prev
	 * @param input
	 */
	public static void check(Class<?> pkg,List<CheckResultInterface> remarks,
			StepMeta stepMeta, RowMetaInterface prev, String[] input) {
		CheckResult cr;

		// See if we have input streams leading to this step!
		if (input.length > 0) {
            cr = new CheckResult(
                    CheckResult.TYPE_RESULT_OK,
                    BaseMessages
                            .getString(pkg,
                                    "MolapStep.Check.StepIsReceivingInfoFromOtherSteps"),
                    stepMeta);
			remarks.add(cr);
		} else {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(pkg,
                            "MolapStep.Check.NoInputReceivedFromOtherSteps"),
                    stepMeta);
			remarks.add(cr);
		}	
		
		// also check that each expected key fields are acually coming
		if (prev!=null && prev.size()>0)
		{
//			String errorMessage = ""; 
			/*boolean errorFound = false; 
			
			if (errorFound)
			{
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, errorMessage, stepMeta);
			}
			else
			{*/
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
                        BaseMessages.getString(pkg,
                                "MolapStep.Check.AllFieldsFoundInInput"),
                        stepMeta);
//			}
			remarks.add(cr);
		}	
		else
		{
            String errorMessage = BaseMessages.getString(pkg,
                    "MolapStep.Check.CouldNotReadFromPreviousSteps") + Const.CR;
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, errorMessage, stepMeta);
			remarks.add(cr);
		}
	}
	
	/**
     * Below method will be used to check whether row is empty or not 
     * 
     * @param row
     * @return row empty 
     *
     */
    public static boolean checkAllValuesAreNull(Object[] row)
    {
        for(int i = 0;i < row.length;i++)
        {
            if(null != row[i])
            {
                return false;
            }
        }
        return true;
    }
    
    /**
     * This method will be used to delete sort temp location is it is exites
     * @throws MolapSortKeyAndGroupByException
     */
    public static void deleteSortLocationIfExists(String tempFileLocation)
            throws MolapSortKeyAndGroupByException
    {
        // create new temp file location where this class 
        //will write all the temp files
        File file = new File(tempFileLocation);
        
        if(file.exists())
        {
            try
            {
                MolapUtil.deleteFoldersAndFiles(file);
            }
            catch(MolapUtilException e)
            {
                LOGGER.error(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        e);
            }
        }
    }
    
    /**
     * Below method will be used to create the store
     * @param schemaName
     * @param cubeName
     * @param tableName
     * @return store location
     * @throws KettleException
     */
    public static String createStoreLocaion(String schemaName, String cubeName,
            String tableName,boolean deleteExistingStore, int currentRestructFolder) throws KettleException
    {
        String tempLocationKey = schemaName+'_'+cubeName;
        String baseStorePath = MolapProperties.getInstance().getProperty(
                tempLocationKey,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        baseStorePath = baseStorePath + File.separator + schemaName
                + File.separator + cubeName;
        int restrctFolderCount = currentRestructFolder/*MolapUtil
                .checkAndReturnNextRestructFolderNumber(baseStorePath, "RS_")*/;
        if(restrctFolderCount == -1)
        {
            restrctFolderCount = 0;
        }
        String baseStorePathWithTableName = baseStorePath + File.separator
                + MolapCommonConstants.RESTRUCTRE_FOLDER + restrctFolderCount
                + File.separator + tableName;
        if(deleteExistingStore)
        {
            File file = new File(baseStorePathWithTableName);
            if(file.exists())
            {
                try
                {
                    MolapUtil.deleteFoldersAndFiles(file);
                }
                catch(MolapUtilException e)
                {
                   throw new KettleException("Problem while deleting the existing aggregate table data in case of Manual Aggregation");
                }
            }
        }
        int counter = MolapUtil
                .checkAndReturnCurrentLoadFolderNumber(baseStorePathWithTableName);
        counter++;
        String basePath = baseStorePathWithTableName + File.separator
                + MolapCommonConstants.LOAD_FOLDER + counter;
        if(new File(basePath).exists())
        {
            counter++;
        }
        basePath = baseStorePathWithTableName + File.separator
                + MolapCommonConstants.LOAD_FOLDER + counter
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        boolean isDirCreated = new File(basePath).mkdirs();
        if(!isDirCreated)
        {
            throw new KettleException("Unable to create dataload directory"
                    + basePath);
        }
        return basePath;
    }
    
    /**
     * 
     * @param aggregator
     * @return
     * 
     */
    public static String getAggType(MeasureAggregator aggregator)
    {
        if(aggregator instanceof SumAggregator)
        {
            return MolapCommonConstants.SUM;
        }
        else if(aggregator instanceof MaxAggregator)
        {
            return MolapCommonConstants.MAX;
        }
        else if(aggregator instanceof MinAggregator)
        {
            return MolapCommonConstants.MIN;
        }
        else if(aggregator instanceof AvgAggregator)
        {
            return MolapCommonConstants.AVERAGE;
        }
        else if(aggregator instanceof CountAggregator)
        {
            return MolapCommonConstants.COUNT;
        }
        else if(aggregator instanceof DistinctCountAggregator)
        {
            return MolapCommonConstants.DISTINCT_COUNT;
        }
        else if(aggregator instanceof SumDistinctAggregator)
        {
            return MolapCommonConstants.SUM_DISTINCT;
        }
        return null;
    }
    
//    /**
//     * 
//     * @param aggregator
//     * @return
//     * 
//     */
//    public static MeasureAggregator getAggregatorObject(String aggregator)
//    {
//        if(aggregator.equals(MolapCommonConstants.SUM))
//        {
//            return new SumAggregator();
//        }
//        else if(aggregator.equals(MolapCommonConstants.MAX))
//        {
//            return new MaxAggregator();
//        }
//        else if(aggregator.equals(MolapCommonConstants.MIN))
//        {
//            return new MinAggregator();
//        }
//        else if(aggregator.equals(MolapCommonConstants.AVERAGE))
//        {
//            return new AvgAggregator();
//        }
//        else if(aggregator.equals(MolapCommonConstants.COUNT))
//        {
//            return new CountAggregator();
//        }
//        else if(aggregator.equals(MolapCommonConstants.DISTINCT_COUNT))
//        {
//            return new DistinctCountAggregator();
//        }
//        return null;
//    }
//    
//    /**
//     * 
//     * @param agg
//     * @return
//     * 
//     */
//    public static boolean isCustomMeasure(String agg)
//    {
//        if(MolapCommonConstants.AVERAGE.equals(agg)
//                || MolapCommonConstants.DISTINCT_COUNT.equals(agg)
//                || MolapCommonConstants.CUSTOM.equals(agg))
//        {
//            return true;
//        }
//        return false;
//    }
    
    /**
     * 
     * @param schema
     * @param cube
     * @param aggreateLevels
     * @return
     * 
     */
    public static String[] getReorderedLevels(MolapDef.Schema schema,
            MolapDef.Cube cube, String[] aggreateLevels, String factTableName)
    {
        String[] factDimensions = MolapSchemaParser.getAllCubeDimensions(cube,
                schema);
        String[] reorderedAggregateLevels = new String[aggreateLevels.length];
        int[] reorderIndex = new int[aggreateLevels.length];
        String aggLevel = null;
        String factName = factTableName + '_';
        for(int i = 0;i < aggreateLevels.length;i++)
        {
            aggLevel = factName + aggreateLevels[i];
            for(int j = 0;j < factDimensions.length;j++)
            {
                if(aggLevel.equals(factDimensions[j]))
                {
                    reorderIndex[i] = j;
                    break;
                }
            }
        }
        Arrays.sort(reorderIndex);
        for(int i = 0;i < reorderIndex.length;i++)
        {
            aggLevel = factDimensions[reorderIndex[i]];
            aggLevel = aggLevel.substring(factName.length());
            reorderedAggregateLevels[i] = aggLevel;
        }
        return reorderedAggregateLevels;
    }
    
    /**
     * This method will provide the updated cardinality based on newly added
     * dimensions
     * 
     * @param factLevels
     * @param aggreateLevels
     * @param factDimCardinality
     * @param newDimesnions
     * @param newDimLens
     * @return
     * 
     */
    public static int[] getKeyGenerator(String[] factLevels,
            String[] aggreateLevels, int[] factDimCardinality,
            String[] newDimesnions, int[] newDimLens)
    {
        int[] factTableDimensioncardinality = new int[factLevels.length];
        System.arraycopy(factDimCardinality, 0, factTableDimensioncardinality,
                0, factDimCardinality.length);
        if(null != newDimesnions)
        {
            for(int j = 0;j < factLevels.length;j++)
            {
                for(int i = 0;i < newDimesnions.length;i++)
                {
                    if(factLevels[j].equals(newDimesnions[i]))
                    {
                        factTableDimensioncardinality[j] = newDimLens[i];
                        break;
                    }
                }
            }
        }
        return factTableDimensioncardinality;
    }
    
    /**
     * 
     * @param factStoreLocation
     * @param currentRestructNumber
     * @return
     * 
     */
    public static SliceMetaData readSliceMetadata(String factStoreLocation,
            int currentRestructNumber)
    {
        String fileLocation = factStoreLocation
                .substring(0, factStoreLocation
                        .indexOf(MolapCommonConstants.LOAD_FOLDER) - 1);
        SliceMetaData sliceMetaData = MolapUtil.readSliceMetaDataFile(
                fileLocation, currentRestructNumber);

        return sliceMetaData;
    }
}
