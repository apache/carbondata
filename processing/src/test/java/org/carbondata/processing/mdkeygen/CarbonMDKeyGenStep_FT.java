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
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 */
package org.carbondata.processing.mdkeygen;

import junit.framework.TestCase;
import org.junit.Test;

/**
 *
 * @author K00900841
 *
 */
public class CarbonMDKeyGenStep_FT extends TestCase {

    @Test
    public void test() {
        assertTrue(true);
    }
    //    private String kettleProperty;
    //
    //    private String aggLevels;
    //
    //    private String tableName;
    //
    //    private String storeLocation;
    //
    //    private String storeLocationPath;
    //
    //    @BeforeClass
    //    public void setUp()
    //    {
    //        File file = new File("");
    //        kettleProperty = System.getProperty("KETTLE_HOME");
    //        System.setProperty("KETTLE_HOME", file.getAbsolutePath() + File.separator + "carbonplugins" + File.separator
    //                + "carbonplugins");
    //        try
    //        {
    //            EnvUtil.environmentInit();
    //            KettleEnvironment.init();
    //        }
    //        catch(KettleException e)
    //        {
    //            assertTrue(false);
    //        }
    //        Schema schema = CarbonSchemaParser.loadXML(file.getAbsolutePath() + File.separator + "test" + File.separator
    //                + "resources" + File.separator + "Vishal5SecondsTest1.xml");
    //        Cube mondrianCubes = CarbonSchemaParser.getMondrianCubes(schema);
    //        Map<String, String> cardinalities = CarbonSchemaParser.getCardinalities(mondrianCubes.dimensions);
    //        String[] cubeDimensions = CarbonSchemaParser.getCubeDimensions(mondrianCubes);
    //        aggLevels = getAggregateLevelCardinalities(cardinalities, cubeDimensions);
    //        tableName = CarbonSchemaParser.getFactTableName(mondrianCubes);
    //        storeLocation = schema.name + File.separator + mondrianCubes.name;
    //        String baseStorelocation = CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL + File.separator + storeLocation;
    //        int counter = CarbonUtil.checkAndReturnNextFolderNumber(baseStorelocation);
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
    //    }
    //
    //    @AfterClass
    //    public void tearDown()
    //    {
    //        if(null != kettleProperty)
    //        {
    //            System.setProperty("KETTLE_HOME", kettleProperty);
    //        }
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
    //    public void test_CarbonMdkeyGenStep_MeasureMetaFile_Output_MeasureMetaFile_With_Max_Min_DecimalValue()
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
    //        StepMeta carbonMDKeyStep = getCarbonMDKeyStep();
    //        transMeta.addStep(carbonMDKeyStep);
    //        TransHopMeta injectorToMdkeyHop = new TransHopMeta(injectorStep, carbonMDKeyStep);
    //        transMeta.addTransHop(injectorToMdkeyHop);
    //
    //        String dummyStepname = "dummy step";
    //        DummyTransMeta dm = new DummyTransMeta();
    //
    //        String dummyPid = registry.getPluginId(StepPluginType.class, dm);
    //        StepMeta dummyStep = new StepMeta(dummyPid, dummyStepname, (StepMetaInterface)dm);
    //        transMeta.addStep(dummyStep);
    //
    //        TransHopMeta mdkeyToDummyStep = new TransHopMeta(carbonMDKeyStep, dummyStep);
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
    //        List<List<byte[]>> intputSurrogateKeysInBytes = new ArrayList<List<byte[]>>();
    //        for(int i = 0;i < 9;i++)
    //        {
    //            intputSurrogateKeysInBytes.add(new ArrayList<byte[]>());
    //        }
    //        List<RowMetaAndData> inputList = createMdKeyInputData(intputSurrogateKeysInBytes);
    //        for(RowMetaAndData rm : inputList)
    //        {
    //            rp.putRow(rm.getRowMeta(), rm.getData());
    //        }
    //        rp.finished();
    //
    //        trans.waitUntilFinished();
    //        File file = new File(new File(storeLocationPath).getAbsolutePath() + File.separator
    //                + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + tableName);
    //        assertTrue(file.exists());
    //        String property = CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL;
    //        String[] split = property.split("/");
    //
    //        File file1 = new File("");
    //        file1 = new File(file1.getAbsolutePath() + File.separator + "../" + split[1]);
    //        delete(file1);
    //
    //    }
    //
    //    public void test_CarbonMdkeyGenStep_Validate_MeasureMetaFile_Output_MeasureMetaFile_With_Proper_Max_Min_DecimalLength_Values()
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
    //        StepMeta carbonMDKeyStep = getCarbonMDKeyStep();
    //        transMeta.addStep(carbonMDKeyStep);
    //        TransHopMeta injectorToMdkeyHop = new TransHopMeta(injectorStep, carbonMDKeyStep);
    //        transMeta.addTransHop(injectorToMdkeyHop);
    //
    //        String dummyStepname = "dummy step";
    //        DummyTransMeta dm = new DummyTransMeta();
    //
    //        String dummyPid = registry.getPluginId(StepPluginType.class, dm);
    //        StepMeta dummyStep = new StepMeta(dummyPid, dummyStepname, (StepMetaInterface)dm);
    //        transMeta.addStep(dummyStep);
    //
    //        TransHopMeta mdkeyToDummyStep = new TransHopMeta(carbonMDKeyStep, dummyStep);
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
    //        List<List<byte[]>> intputSurrogateKeysInBytes = new ArrayList<List<byte[]>>();
    //        for(int i = 0;i < 9;i++)
    //        {
    //            intputSurrogateKeysInBytes.add(new ArrayList<byte[]>());
    //        }
    //
    //        List<RowMetaAndData> inputList = createMdKeyInputData(intputSurrogateKeysInBytes);
    //
    //        for(RowMetaAndData rm : inputList)
    //        {
    //            rp.putRow(rm.getRowMeta(), rm.getData());
    //        }
    //        rp.finished();
    //
    //        trans.waitUntilFinished();
    //        File file = new File(new File(storeLocationPath).getAbsolutePath() + File.separator
    //                + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + tableName);
    //        assertTrue(file.exists());
    //        checkMeasureFile(file.getAbsolutePath(), 10);
    //
    //        String property = CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL;
    //        String[] split = property.split("/");
    //        File file1 = new File("");
    //        file1 = new File(file1.getAbsolutePath() + File.separator + "../" + split[1]);
    //        delete(file1);
    //    }
    //    public void test_CarbonMdkeyGenStep_Validate_Output_MDkeyGenStep_Generating_Proper_MDkey_For_Input_Surrogate_Keys()
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
    //        StepMeta carbonMDKeyStep = getCarbonMDKeyStep();
    //        transMeta.addStep(carbonMDKeyStep);
    //        TransHopMeta injectorToMdkeyHop = new TransHopMeta(injectorStep, carbonMDKeyStep);
    //        transMeta.addTransHop(injectorToMdkeyHop);
    //
    //        String dummyStepname = "dummy step";
    //        DummyTransMeta dm = new DummyTransMeta();
    //
    //        String dummyPid = registry.getPluginId(StepPluginType.class, dm);
    //        StepMeta dummyStep = new StepMeta(dummyPid, dummyStepname, (StepMetaInterface)dm);
    //        transMeta.addStep(dummyStep);
    //
    //        TransHopMeta mdkeyToDummyStep = new TransHopMeta(carbonMDKeyStep, dummyStep);
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
    //        List<List<byte[]>> intputSurrogateKeysInBytes = new ArrayList<List<byte[]>>();
    //        for(int i = 0;i < 9;i++)
    //        {
    //            intputSurrogateKeysInBytes.add(new ArrayList<byte[]>());
    //        }
    //
    //        List<RowMetaAndData> inputList = createMdKeyInputData(intputSurrogateKeysInBytes);
    //
    //        long[][] expectedSurrogateKeyArray = new long[9][11];
    //
    //        for(int i = 0;i < intputSurrogateKeysInBytes.size();i++)
    //        {
    //            List<byte[]> list = intputSurrogateKeysInBytes.get(i);
    //
    //            for(int j = 0;j < list.size();j++)
    //            {
    //                expectedSurrogateKeyArray[i][j] = Long.parseLong(new String(list.get(j)));
    //            }
    //        }
    //        Arrays.sort(expectedSurrogateKeyArray,new SurrogateKeyComparator());
    //
    //        for(RowMetaAndData rm : inputList)
    //        {
    //            rp.putRow(rm.getRowMeta(), rm.getData());
    //        }
    //        rp.finished();
    //
    //        trans.waitUntilFinished();
    //        List<RowMetaAndData> resultRows = dummyRc.getRowsWritten();
    //        List<byte[]> actualMdkeyList = checkRows(resultRows);
    //        int[] dimLens = getDimLens(aggLevels);
    //        KeyGenerator keyGenerator = getKeyGenerator(dimLens);
    //
    //        long[][] actualSurrogateKeyArray = new long[9][11];
    //
    ////        Collections.sort(actualMdkeyList,new MDKeyComparator());
    ////        for(int i = 0;i < actualMdkeyList.size();i++)
    ////        {
    ////            System.out.println(Arrays.toString(actualMdkeyList.get(i)));
    ////        }
    //        for(int i = 0;i < actualSurrogateKeyArray.length;i++)
    //        {
    //            actualSurrogateKeyArray[i] = keyGenerator.getKeyArray(actualMdkeyList.get(i));
    //        }
    //
    //        Arrays.sort(actualSurrogateKeyArray,new SurrogateKeyComparator());
    //
    //        for(int i = 0;i < actualSurrogateKeyArray.length;i++)
    //        {
    //            assertEquals(Arrays.toString(actualSurrogateKeyArray[i]), Arrays.toString(expectedSurrogateKeyArray[i]));
    //        }
    //
    //        String property = CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL;
    //        String[] split = property.split("/");
    //
    //        File file1 = new File("");
    //        file1 = new File(file1.getAbsolutePath() + File.separator + "../" + split[1]);
    //        delete(file1);
    //    }
    //    /**
    //     * Check the list, the list has to be sorted.
    //     */
    //    private List<byte[]> checkRows(List<RowMetaAndData> rows)
    //    {
    //        List<byte[]> mdkeyList = new ArrayList<byte[]>();
    //
    //        for(RowMetaAndData rMD : rows)
    //        {
    //            mdkeyList.add((byte[])rMD.getData()[10]);
    //        }
    //        return mdkeyList;
    //    }
    //
    //    private class SurrogateKeyComparator implements Comparator<long[]>
    //    {
    //        @Override
    //        public int compare(long[] b1, long[] b2)
    //        {
    //            int cmp = 0;
    //            int length = b1.length < b2.length ? b1.length : b2.length;
    //
    //            for(int i = 0;i < length;i++)
    //            {
    //                long a = b1[i];
    //                long b = b2[i];
    //                cmp =(int) (a - b);
    //                if(cmp != 0)
    //                {
    //                    cmp = cmp < 0 ? -1 : 1;
    //                    break;
    //                }
    //            }
    //            if(b1.length != b2.length && cmp == 0)
    //            {
    //                cmp = b1.length - b2.length;
    //            }
    //            return cmp;
    //        }
    //
    //    }
    //
    //    private class MDKeyComparator implements Comparator<byte[]>
    //    {
    //        @Override
    //        public int compare(byte[] b1, byte[] b2)
    //        {
    //            int cmp = 0;
    //            int length = b1.length < b2.length ? b1.length : b2.length;
    //
    //            for(int i = 0;i < length;i++)
    //            {
    //                int a = (b1[i] & 0xff);
    //                int b = (b2[i] & 0xff);
    //                cmp = a - b;
    //                if (cmp!=0)
    //                {
    //                    cmp = cmp < 0 ? -1 : 1;
    //                    break;
    //                }
    //            }
    //            if(b1.length != b2.length && cmp == 0)
    //            {
    //                cmp = b1.length - b2.length;
    //            }
    //            return cmp;
    //        }
    //
    //    }
    //
    //    private void checkMeasureFile(String measureMetaDataFileLocation, int measureCount)
    //
    //    {
    //        FileInputStream stream = null;
    //        FileChannel channel = null;
    //        double[] maxValue = new double[measureCount];
    //        double[] minValue = new double[measureCount];
    //        int[] decimalLength = new int[measureCount];
    //        int currentIndex = 0;
    //        int totalSize = measureCount * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE * 2 + measureCount
    //                * CarbonCommonConstants.INT_SIZE_IN_BYTE;
    //        ByteBuffer allocate = ByteBuffer.allocate(totalSize);
    //        try
    //        {
    //            stream = new FileInputStream(measureMetaDataFileLocation);
    //            channel = stream.getChannel();
    //            channel.read(allocate);
    //        }
    //        catch(IOException exception)
    //        {
    //        }
    //        finally
    //        {
    //            closeable(channel);
    //            closeable(stream);
    //
    //        }
    //        allocate.rewind();
    //        for(int i = 0;i < maxValue.length;i++)
    //        {
    //            maxValue[currentIndex++] = allocate.getDouble();
    //        }
    //        currentIndex = 0;
    //        for(int i = 0;i < minValue.length;i++)
    //        {
    //            minValue[currentIndex++] = allocate.getDouble();
    //        }
    //        currentIndex = 0;
    //        for(int i = 0;i < decimalLength.length;i++)
    //        {
    //            decimalLength[currentIndex++] = allocate.getInt();
    //        }
    //        assertEquals("[23.1, 23.1, 23.1, 23.1, 23.1, 23.1, 23.1, 23.1, 23.1, 23.1]", Arrays.toString(minValue));
    //        assertEquals("[107.1, 107.1, 107.1, 107.1, 107.1, 107.1, 107.1, 107.1, 107.1, 107.1]",
    //                Arrays.toString(maxValue));
    //        assertEquals("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]", Arrays.toString(decimalLength));
    //    }
    //
    //    /**
    //     * This method will be used to close the streams
    //     *
    //     * @param stream
    //     *            stream
    //     *
    //     */
    //    private static void closeable(Closeable stream)
    //    {
    //        // check whether stream is null or not
    //        if(null != stream)
    //        {
    //            try
    //            {
    //                stream.close();
    //            }
    //            catch(IOException e)
    //            {
    //                // TODO Add logger
    //            }
    //        }
    //    }
    //
    //    private List<RowMetaAndData> createMdKeyInputData(List<List<byte[]>> intputSurrogateKeysInBytes)
    //    {
    //        List<RowMetaAndData> list = new ArrayList<RowMetaAndData>(9);
    //        RowMetaInterface rm = createRowMetaInterface();
    //        double d = 23.1;
    //        for(int i = 0;i < 9;i++)
    //        {
    //            Object[] r1 = new Object[21];
    //            int len = 0;
    //            byte array[][] = {{getSurrogateKeyForDeviceName()}, {getSurrogateKeyForYear()},
    //                    {getSurrogateKeyForMonth()}, {getSurrogateKeyForDay()}, {getSurrogateKeyForHour()},
    //                    {getSurrogateKeyForMinute()}, {getSurrogateKeyForCountry()}, {getSurrogateKeyForState()},
    //                    {getSurrogateKeyForCity()}, {getSurrogateKeyForCategory_Name()},
    //                    {getSurrogateKeyForProtocol_Name()}};
    //            for(int j = 0;j < 11;j++)
    //            {
    //
    //                r1[len++] = array[j];
    //                intputSurrogateKeysInBytes.get(i).add(array[j]);
    //            }
    //            for(int j = 0;j < 10;j++)
    //            {
    //                r1[len++] = d;
    //            }
    //            d = d + 10.5;
    //            list.add(new RowMetaAndData(rm, r1));
    //        }
    //        return list;
    //    }
    //
    //    private byte getSurrogateKeyForDeviceName()
    //    {
    //        int min = 49;
    //        int max = 57;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForMonth()
    //    {
    //        int min = 49;
    //        int max = 57;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForYear()
    //    {
    //        int min = 49;
    //        int max = 50;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForDay()
    //    {
    //        int min = 49;
    //        int max = 57;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForHour()
    //    {
    //        int min = 49;
    //        int max = 57;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForMinute()
    //    {
    //        int min = 49;
    //        int max = 52;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForCountry()
    //    {
    //        int min = 49;
    //        int max = 53;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForState()
    //    {
    //        int min = 49;
    //        int max = 57;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForCity()
    //    {
    //        int min = 49;
    //        int max = 57;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForCategory_Name()
    //    {
    //        int min = 49;
    //        int max = 53;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //
    //    private byte getSurrogateKeyForProtocol_Name()
    //    {
    //        int min = 49;
    //        int max = 57;
    //        Random r = new Random();
    //        int i1 = r.nextInt(max - min + 1) + min;
    //        return (byte)i1;
    //    }
    //    private RowMetaInterface createRowMetaInterface()
    //    {
    //        RowMetaInterface rm = new RowMeta();
    //        ValueMetaInterface valuesMeta[] = new ValueMeta[21];
    //        int len = 0;
    //        for(int i = 0;i < 11;i++)
    //        {
    //            valuesMeta[len] = new ValueMeta("KEY" + len, ValueMetaInterface.TYPE_STRING,
    //                    ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
    //            valuesMeta[len].setStorageMetadata((new ValueMeta("KEY" + len, ValueMetaInterface.TYPE_STRING,
    //                    ValueMetaInterface.STORAGE_TYPE_NORMAL)));
    //            len++;
    //        }
    //        for(int i = 0;i < 10;i++)
    //        {
    //            valuesMeta[len] = new ValueMeta("KEY" + len, ValueMeta.TYPE_NUMBER);
    //            len++;
    //        }
    //        for(int i = 0;i < valuesMeta.length;i++)
    //        {
    //            rm.addValueMeta(valuesMeta[i]);
    //        }
    //
    //        return rm;
    //    }
    //
    //    private KeyGenerator getKeyGenerator(int[] dimLens)
    //    {
    //        int[] lens = new int[dimLens.length];
    //
    //        for(int i = 0;i < lens.length;i++)
    //        {
    //            lens[i] = Long.toBinaryString(dimLens[i]).length();
    //        }
    //
    //        return new MultiDimKeyVarLengthGenerator(lens);
    //    }
    //
    //    private int[] getDimLens(String dimensions)
    //    {
    //        String[] dims = dimensions.split(",");
    //        int[] dimLens = new int[dims.length];
    //        for(int i = 0;i < dims.length;i++)
    //        {
    //            dimLens[i] = Integer.parseInt(dims[i].split(":")[1]);
    //        }
    //
    //        return dimLens;
    //    }
    //
    //    private StepMeta getCarbonMDKeyStep()
    //    {
    //        CarbonMDKeyGenStepMeta carbonMdKey = new CarbonMDKeyGenStepMeta();
    //        carbonMdKey.setNumberOfCores("2");
    //        carbonMdKey.setTableName(tableName);
    //        carbonMdKey.setStoreLocation(storeLocation);
    //        carbonMdKey.setAggregateLevels(aggLevels);
    //        StepMeta mdkeyStepMeta = new StepMeta(GraphGeneratorConstants.CARBON_MDKEY_GENERATOR,
    //                (StepMetaInterface)carbonMdKey);
    //        mdkeyStepMeta.setName("CarbonMDKeyGen");
    //        mdkeyStepMeta.setStepID("CarbonMDKeyGen");
    //        mdkeyStepMeta.setDescription("Generate MDKey For Table Data: " + GraphGeneratorConstants.CARBON_MDKEY_GENERATOR);
    //
    //        return mdkeyStepMeta;
    //    }
    //
    //    private String getAggregateLevelCardinalities(Map<String, String> dimCardinalities, String[] aggDims)
    //    {
    //        StringBuilder sb = new StringBuilder();
    //        for(int i = 0;i < aggDims.length;i++)
    //        {
    //            String string = dimCardinalities.get(aggDims[i]);
    //            if(string != null)
    //            {
    //                sb.append(aggDims[i]).append(":").append(string).append(",");
    //            }
    //        }
    //        String resultStr = sb.toString();
    //        if(resultStr.endsWith(","))
    //        {
    //            resultStr = resultStr.substring(0, resultStr.length() - 1);
    //        }
    //        return resultStr;
    //    }
}
