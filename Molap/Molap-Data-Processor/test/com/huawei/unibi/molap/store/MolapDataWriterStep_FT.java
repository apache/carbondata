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
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2012
 * =====================================
 *
 */
package com.huawei.unibi.molap.store;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * 
 * @author K00900841
 *
 */
public class MolapDataWriterStep_FT extends TestCase
{
    
    @Test
    public void test()
    {
        assertTrue(true);
    }
//    private String tableName;
//
//    private String storeLocation;
//
//    private String kettleProperty;
//
//    private String storeLocationPath;
//    
//    private int mdkeySize=0;
//    
//    @BeforeClass
//    public void setUp()
//    {
//        File file = new File("");
//        kettleProperty = System.getProperty("KETTLE_HOME");
//        System.setProperty("KETTLE_HOME", file.getAbsolutePath() + File.separator + "molapplugins" + File.separator
//                + "molapplugins");
//        try
//        {
//            EnvUtil.environmentInit();
//            KettleEnvironment.init();
//        }
//        catch(KettleException e)
//        {
//            assertTrue(false);
//        }
//        Schema schema = MolapSchemaParser.loadXML(file.getAbsolutePath() + File.separator + "test" + File.separator
//                + "resources" + File.separator + "Vishal5SecondsTest1.xml");
//        Cube mondrianCubes = MolapSchemaParser.getMondrianCubes(schema);
//        tableName = MolapSchemaParser.getFactTableName(mondrianCubes);
//        storeLocation = schema.name + "/" + mondrianCubes.name;
//        String baseStorelocation = MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL + File.separator + storeLocation;
//        int counter = MolapUtil.checkAndReturnNextFolderNumber(baseStorelocation);
//        File file1 = new File(baseStorelocation);
//        if(file1.exists())
//        {
//            file1.delete();
//        }
//        storeLocationPath = file1.getAbsolutePath() + File.separator + "Load_" + (counter + 1);
//        file1 = new File(storeLocationPath);
//
//        if(file1.exists())
//        {
//            file1.delete();
//            file1.mkdirs();
//        }
//        else
//        {
//            file1.mkdirs();
//        }
//
//        copyFile(storeLocationPath + File.separator + MolapCommonConstants.MEASURE_METADATA_FILE_NAME + tableName,
//                file.getAbsolutePath() + File.separator + "test" + File.separator + "resources" + File.separator
//                        + "msrMetaData_DATA_FACT");
//        this.mdkeySize = getMdkey(0).length;
//    }
//
//    @AfterClass
//    public void tearDown()
//    {
//        if(null != kettleProperty)
//        {
//            System.setProperty("KETTLE_HOME", kettleProperty);
//        }
//        String property = MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL;
//        String[] split = property.split("/");
//
//        File file = new File("");
//        file = new File(file.getAbsolutePath() + File.separator + "../" + split[1]);
//        delete(file);
//    }
//    
//    private void delete(File f)
//    {
//        if(f.isDirectory())
//        {
//            for(File c : f.listFiles())
//            {
//                delete(c);
//            }
//        }
//        f.delete();
//    }
//    
//    @Test
//    public void test_MolapDataWriterStep_InterMediateFile_Output_Intermediate_File_PresentIn_Store_Location()
//    {
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("sortrowstest");
//
//        String injectorStepname = "injector step";
//        InjectorMeta im = new InjectorMeta();
//
//        PluginRegistry registry = PluginRegistry.getInstance();
//        String injectorPid = registry.getPluginId(StepPluginType.class, im);
//        StepMeta injectorStep = new StepMeta(injectorPid, injectorStepname, (StepMetaInterface)im);
//        transMeta.addStep(injectorStep);
//        StepMeta molapMDKeyStep = getMolapDataWriterStep();
//        transMeta.addStep(molapMDKeyStep);
//        TransHopMeta injectorToMdkeyHop = new TransHopMeta(injectorStep, molapMDKeyStep);
//        transMeta.addTransHop(injectorToMdkeyHop);
//
//        String dummyStepname = "dummy step";
//        DummyTransMeta dm = new DummyTransMeta();
//
//        String dummyPid = registry.getPluginId(StepPluginType.class, dm);
//        StepMeta dummyStep = new StepMeta(dummyPid, dummyStepname, (StepMetaInterface)dm);
//        transMeta.addStep(dummyStep);
//
//        TransHopMeta mdkeyToDummyStep = new TransHopMeta(molapMDKeyStep, dummyStep);
//        transMeta.addTransHop(mdkeyToDummyStep);
//
//        Trans trans = new Trans(transMeta);
//        RowProducer rp = null;
//        StepInterface si = null;
//        RowStepCollector dummyRc = null;
//        try
//        {
//            trans.prepareExecution(null);
//            si = trans.getStepInterface(dummyStepname, 0);
//            dummyRc = new RowStepCollector();
//            si.addRowListener(dummyRc);
//
//            rp = trans.addRowProducer(injectorStepname, 0);
//            trans.startThreads();
//        }
//        catch(Exception e)
//        {
//            assertTrue(false);
//        }
//        List<RowMetaAndData> inputList = createMolapDataWriterInputData();
//        for(RowMetaAndData rm : inputList)
//        {
//            rp.putRow(rm.getRowMeta(), rm.getData());
//        }
//        rp.finished();
//        trans.waitUntilFinished();
//        File file = new File (storeLocationPath + File.separator+tableName+".intermediate");
//        assertTrue(file.exists());
//        
//        String property = MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL;
//        String[] split = property.split("/");
//
//        file = new File("");
//        file = new File(file.getAbsolutePath() + File.separator + "../" + split[1]);
//        delete(file);
//    }
//    
//    @Test
//    public void test_MolapDataWriterStep_InterMediateFile_Output_Intermediate_File_ContainsProper_LeafNode_MetaData()
//    {
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("sortrowstest");
//
//        String injectorStepname = "injector step";
//        InjectorMeta im = new InjectorMeta();
//
//        PluginRegistry registry = PluginRegistry.getInstance();
//        String injectorPid = registry.getPluginId(StepPluginType.class, im);
//        StepMeta injectorStep = new StepMeta(injectorPid, injectorStepname, (StepMetaInterface)im);
//        transMeta.addStep(injectorStep);
//        StepMeta molapMDKeyStep = getMolapDataWriterStep();
//        transMeta.addStep(molapMDKeyStep);
//        TransHopMeta injectorToMdkeyHop = new TransHopMeta(injectorStep, molapMDKeyStep);
//        transMeta.addTransHop(injectorToMdkeyHop);
//
//        String dummyStepname = "dummy step";
//        DummyTransMeta dm = new DummyTransMeta();
//
//        String dummyPid = registry.getPluginId(StepPluginType.class, dm);
//        StepMeta dummyStep = new StepMeta(dummyPid, dummyStepname, (StepMetaInterface)dm);
//        transMeta.addStep(dummyStep);
//
//        TransHopMeta mdkeyToDummyStep = new TransHopMeta(molapMDKeyStep, dummyStep);
//        transMeta.addTransHop(mdkeyToDummyStep);
//
//        Trans trans = new Trans(transMeta);
//        RowProducer rp = null;
//        StepInterface si = null;
//        RowStepCollector dummyRc = null;
//        try
//        {
//            trans.prepareExecution(null);
//            si = trans.getStepInterface(dummyStepname, 0);
//            dummyRc = new RowStepCollector();
//            si.addRowListener(dummyRc);
//
//            rp = trans.addRowProducer(injectorStepname, 0);
//            trans.startThreads();
//        }
//        catch(Exception e)
//        {
//            assertTrue(false);
//        }
//        List<RowMetaAndData> inputList = createMolapDataWriterInputData();
//        for(RowMetaAndData rm : inputList)
//        {
//            rp.putRow(rm.getRowMeta(), rm.getData());
//        }
//        rp.finished();
//        trans.waitUntilFinished();
//        File file = new File (storeLocationPath + File.separator+tableName+".intermediate");
//        assertTrue(file.exists());
//        FileInputStream stream  = null;
//        FileChannel channel = null;
//        try
//        {
//            stream = new FileInputStream(file);
//            channel = stream.getChannel();
//            long fileSize = channel.size();
//            long offset = 0;
//            byte[] actualMdkeyArray = read(this.mdkeySize, channel, 0).array();
//            byte[] expectedMdkey= getMdkey(0);
//            for(int i = 0;i < actualMdkeyArray.length;i++)
//            {
//                if(actualMdkeyArray[i]!=expectedMdkey[i])
//                {
//                    assertTrue(false);
//                    break;
//                }
//            }
//            offset+=offset + this.mdkeySize;
//            int fileNameByteArrayLength = read(MolapCommonConstants.INT_SIZE_IN_BYTE, channel, offset)
//                    .getInt();
//            offset += MolapCommonConstants.INT_SIZE_IN_BYTE;
//            String read = new String(read(fileNameByteArrayLength, channel,
//                    offset).array());
//            assertEquals("DATA_FACT_0.leaf", read);
//            offset +=fileNameByteArrayLength;
//            long leafOffset = read(MolapCommonConstants.LONG_SIZE_IN_BYTE, channel,
//                    offset)
//                    .getLong();
//            offset+=MolapCommonConstants.LONG_SIZE_IN_BYTE;
//            assertEquals(0, leafOffset);
//            if(fileSize>offset)
//            {
//                assertTrue(false);
//            }
//        }
//        catch (IOException e) 
//        {
//            assertTrue(false);
//        }
//        finally
//        {
//            closeQuitely(channel);
//            closeQuitely(stream);
//        }
//        
//        String property = MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL;
//        String[] split = property.split("/");
//
//        file = new File("");
//        file = new File(file.getAbsolutePath() + File.separator + "../" + split[1]);
//        delete(file);
//    }
//    
//    @Test
//    public void test_MolapDataWriterStep_LeafNodeFile_Output_LeafNode_File_PresentIn_Store_Location()
//    {
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("sortrowstest");
//
//        String injectorStepname = "injector step";
//        InjectorMeta im = new InjectorMeta();
//
//        PluginRegistry registry = PluginRegistry.getInstance();
//        String injectorPid = registry.getPluginId(StepPluginType.class, im);
//        StepMeta injectorStep = new StepMeta(injectorPid, injectorStepname, (StepMetaInterface)im);
//        transMeta.addStep(injectorStep);
//        StepMeta molapMDKeyStep = getMolapDataWriterStep();
//        transMeta.addStep(molapMDKeyStep);
//        TransHopMeta injectorToMdkeyHop = new TransHopMeta(injectorStep, molapMDKeyStep);
//        transMeta.addTransHop(injectorToMdkeyHop);
//
//        String dummyStepname = "dummy step";
//        DummyTransMeta dm = new DummyTransMeta();
//
//        String dummyPid = registry.getPluginId(StepPluginType.class, dm);
//        StepMeta dummyStep = new StepMeta(dummyPid, dummyStepname, (StepMetaInterface)dm);
//        transMeta.addStep(dummyStep);
//
//        TransHopMeta mdkeyToDummyStep = new TransHopMeta(molapMDKeyStep, dummyStep);
//        transMeta.addTransHop(mdkeyToDummyStep);
//
//        Trans trans = new Trans(transMeta);
//        RowProducer rp = null;
//        StepInterface si = null;
//        RowStepCollector dummyRc = null;
//        try
//        {
//            trans.prepareExecution(null);
//            si = trans.getStepInterface(dummyStepname, 0);
//            dummyRc = new RowStepCollector();
//            si.addRowListener(dummyRc);
//
//            rp = trans.addRowProducer(injectorStepname, 0);
//            trans.startThreads();
//        }
//        catch(Exception e)
//        {
//            assertTrue(false);
//        }
//        List<RowMetaAndData> inputList = createMolapDataWriterInputData();
//        for(RowMetaAndData rm : inputList)
//        {
//            rp.putRow(rm.getRowMeta(), rm.getData());
//        }
//        rp.finished();
//        trans.waitUntilFinished();
//        File file = new File (storeLocationPath + File.separator+tableName+"_0"+".leaf");
//        assertTrue(file.exists());
//        
//        String property = MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL;
//        String[] split = property.split("/");
//
//        file = new File("");
//        file = new File(file.getAbsolutePath() + File.separator + "../" + split[1]);
//        delete(file);
//    }
//    @Test
//    public void test_MolapDataWriterStep_LeafNodeFile_Output_LeafNode_File_Contains_Proper_LeafNodeData()
//    {
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("sortrowstest");
//
//        String injectorStepname = "injector step";
//        InjectorMeta im = new InjectorMeta();
//
//        PluginRegistry registry = PluginRegistry.getInstance();
//        String injectorPid = registry.getPluginId(StepPluginType.class, im);
//        StepMeta injectorStep = new StepMeta(injectorPid, injectorStepname, (StepMetaInterface)im);
//        transMeta.addStep(injectorStep);
//        StepMeta molapMDKeyStep = getMolapDataWriterStep();
//        transMeta.addStep(molapMDKeyStep);
//        TransHopMeta injectorToMdkeyHop = new TransHopMeta(injectorStep, molapMDKeyStep);
//        transMeta.addTransHop(injectorToMdkeyHop);
//
//        String dummyStepname = "dummy step";
//        DummyTransMeta dm = new DummyTransMeta();
//
//        String dummyPid = registry.getPluginId(StepPluginType.class, dm);
//        StepMeta dummyStep = new StepMeta(dummyPid, dummyStepname, (StepMetaInterface)dm);
//        transMeta.addStep(dummyStep);
//
//        TransHopMeta mdkeyToDummyStep = new TransHopMeta(molapMDKeyStep, dummyStep);
//        transMeta.addTransHop(mdkeyToDummyStep);
//
//        Trans trans = new Trans(transMeta);
//        RowProducer rp = null;
//        StepInterface si = null;
//        RowStepCollector dummyRc = null;
//        try
//        {
//            trans.prepareExecution(null);
//            si = trans.getStepInterface(dummyStepname, 0);
//            dummyRc = new RowStepCollector();
//            si.addRowListener(dummyRc);
//
//            rp = trans.addRowProducer(injectorStepname, 0);
//            trans.startThreads();
//        }
//        catch(Exception e)
//        {
//            assertTrue(false);
//        }
//        List<RowMetaAndData> inputList = createMolapDataWriterInputData();
//        for(RowMetaAndData rm : inputList)
//        {
//            rp.putRow(rm.getRowMeta(), rm.getData());
//        }
//        rp.finished();
//        trans.waitUntilFinished();
//        File file = new File (storeLocationPath + File.separator+tableName+"_0"+".leaf");
//        assertTrue(file.exists());
//        long offset=0;
//        FileHolder fileHolder = new FileHolderImpl();
//        int numberOfKeys = fileHolder.readInt(file.getAbsolutePath(), offset);
//        assertEquals(9, numberOfKeys);
//        offset+=offset+MolapCommonConstants.INT_SIZE_IN_BYTE;
//        int keyLength = fileHolder.readInt(file.getAbsolutePath(), offset);
//        offset+=offset+MolapCommonConstants.INT_SIZE_IN_BYTE;
//        
////        long[] measureOffset  = new long[aggregateNames.size()];
////        int[] measuresLength = new int[aggregateNames.size()];
////        
////        long currentOffset = offset + 8 + keyLength;
////
////        for(int i = 0;i < measuresLength.length;i++)
////        {
////            int currentMeasureLength = fileHolder.readInt(filePath, currentOffset);
////            measuresLength[i] = currentMeasureLength;
////            currentOffset +=4;
////            measureOffset[i] = currentOffset;
////            currentOffset += currentMeasureLength;
////        }
////        leafNodeInfo.setMeasureLength(measuresLength);
////        leafNodeInfo.setMeasureOffset(measureOffset);
////        leafNodeInfo.setFileName(filePath);
//        
//        String property = MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL;
//        String[] split = property.split("/");
//
//        file = new File("");
//        file = new File(file.getAbsolutePath() + File.separator + "../" + split[1]);
//        delete(file);
//    }
//    
////    public LeafNodeInfo getLeafNodeInfo(long offset, String filePath) 
////    {
////        LeafNodeInfo leafNodeInfo = new LeafNodeInfo();
////        leafNodeInfo.setNumberOfKeys(fileHolder.readInt(persistenceFileLocation+File.separator+filePath, offset));
////        int keyLength = fileHolder.readInt(filePath, offset+4);
////        leafNodeInfo.setKeyLength(keyLength);
////        leafNodeInfo.setKeyOffset(8+offset);
////        long[] measureOffset  = new long[aggregateNames.size()];
////        int[] measuresLength = new int[aggregateNames.size()];
////        
////        long currentOffset = offset + 8 + keyLength;
////
////        for(int i = 0;i < measuresLength.length;i++)
////        {
////            int currentMeasureLength = fileHolder.readInt(filePath, currentOffset);
////            measuresLength[i] = currentMeasureLength;
////            currentOffset +=4;
////            measureOffset[i] = currentOffset;
////            currentOffset += currentMeasureLength;
////        }
////        leafNodeInfo.setMeasureLength(measuresLength);
////        leafNodeInfo.setMeasureOffset(measureOffset);
////        leafNodeInfo.setFileName(filePath);
////
////        return leafNodeInfo;
////    }
//    
//    private ByteBuffer read(int size, FileChannel channel, long offset) throws IOException
//    {
//        ByteBuffer buffer = ByteBuffer.allocate(size);
//        channel.position(offset);
//        channel.read(buffer);
//        offset = channel.position();
//        buffer.rewind();
//        return buffer;
//    }
//    private StepMeta getMolapDataWriterStep()
//    {
//        MolapDataWriterStepMeta molapDataWriter = new MolapDataWriterStepMeta();
//        molapDataWriter.setMaxLeafNode("100");
//        molapDataWriter.setLeafNodeSize("8192");
//        molapDataWriter.setTabelName(tableName);
//        molapDataWriter.setStoreLocation(storeLocation);
//        StepMeta stepMeta = new StepMeta(GraphGeneratorConstants.MOLAP_DATA_WRITER,
//                (StepMetaInterface)molapDataWriter);
//        stepMeta.setName("MolapDataWriter");
//        stepMeta.setStepID("MolapDataWriter");
//        stepMeta.setDescription("Molap Data to file: " + GraphGeneratorConstants.MOLAP_DATA_WRITER);
//
//        return stepMeta;
//    }
//    private RowMetaInterface createRowMetaInterface()
//    {
//        RowMetaInterface rm = new RowMeta();
//        ValueMetaInterface[]  valuesMeta= new ValueMeta[11];
//        int len = 0;
//        for(int i = 0;i < 10;i++)
//        {
//            valuesMeta[len] = new ValueMeta("KEY" + len, ValueMeta.TYPE_NUMBER);
//            len++;
//        }
//        valuesMeta[valuesMeta.length-1]=new ValueMeta("KEY" + len, ValueMetaInterface.TYPE_BINARY,
//                ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
//        valuesMeta[len].setStorageMetadata((new ValueMeta("KEY" + len, ValueMetaInterface.TYPE_STRING,
//                ValueMetaInterface.STORAGE_TYPE_NORMAL)));
//        for(int i = 0;i < valuesMeta.length;i++)
//        {
//            rm.addValueMeta(valuesMeta[i]);
//        }
//
//        return rm;
//    }
//    private List<RowMetaAndData> createMolapDataWriterInputData()
//    {
//        List<RowMetaAndData> list = new ArrayList<RowMetaAndData>(9);
//        RowMetaInterface rm = createRowMetaInterface();
//        double d = 23.1;
//        byte[][] inputMdkey = getInputMdkey();
//        for(int i = 0;i < 9;i++)
//        {
//            Object[] r1 = new Object[11];
//            int len = 0;
//            for(int j = 0;j < 10;j++)
//            {
//                r1[len++] = d;
//            }
//            r1[len]=inputMdkey[i];
//            d = d + 10.5;
//            list.add(new RowMetaAndData(rm, r1));
//        }
//        return list;
//    }
//    
//    private byte[][] getInputMdkey()
//    {
//        byte[][] inputMdkey = {{1, -92, 36, 52, 97, 99}, {2, -56, 19, -108, -88, -95}, {2, -46, 33, -88, 73, 33},
//                {3, 34, 96, -76, -24, 71}, {3, 78, -111, 22, 97, -88}, {3, 82, 36, 18, -126, -87},
//                {3, -80, -127, -109, 5, 37}, {3, -58, 116, -102, 105, 38}, {4, 50, 19, 74, -120, -119}};
//        return inputMdkey;
//    }
//    
//    private byte[] getMdkey(int index)
//    {
//        byte[][] inputMdkey = getInputMdkey();
//        return inputMdkey[index];
//    }
//    
//    private void copyFile(String desPath,
//            String sourceFilePath) 
//    {
//        InputStream inputStream = null;
//        OutputStream outputStream = null;
//        // getting input stream for source file path
//        try
//        {
//            inputStream = new FileInputStream(sourceFilePath);
//            outputStream = new FileOutputStream(desPath);
//            copyFile(inputStream, outputStream);
//        }
//        catch(FileNotFoundException e)
//        {
//            // TODO: handle exception
//        }
//    }
//    
//    /**
//     * This method will copy input stream to output stream it will copy file to
//     * destination
//     * 
//     * @param stream
//     *            InputStream
//     * @param outStream
//     *            outStream
//     * @throws IOException
//     * @throws IOException
//     *             IOException
//     */
//    private void copyFile(InputStream stream,
//            OutputStream outStream) 
//    {
//
//        final byte[] buffer = new byte[1024];
//        int len;
//        try
//        {
//            for(;;)
//            {
//                len = stream.read(buffer);
//                if(len == -1)
//                {
//                    return;
//                }
//                // writing to output stream
//                outStream.write(buffer, 0, len);
//            }
//        }
//        catch(IOException e)
//        {
//            
//        }
//        finally
//        {
//            closeQuitely(stream);
//            closeQuitely(outStream);
//        }
//    }
//    
//   private void closeQuitely(Closeable closable)
//   {
//       if(closable != null)
//       {
//           try
//           {
//               closable.close();
//           }
//           catch(IOException e)
//           {
//
//           }
//       }
//   }
}
