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
package com.huawei.unibi.molap.graphgenerator;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor Author K00900841
 * Created Date :21-May-2013 6:42:29 PM FileName : GraphGenerator_UT Class
 * Description : UT class for GraphGenerator Version 1.0
 */
public class GraphGenerator_UT extends TestCase
{
    @Test
    public void test()
    {
        assertTrue(true);
    }
//    private static final String OUTPUT_LOCATION = "../unibi-solutions/system/molap/etl";
//    
//    private GraphGenerator graphGenerator;
//
//    private String kettleProperty;
//    @BeforeClass
//    public void setUp()
//    {
//        File file = new File("");
//        kettleProperty = System.getProperty("KETTLE_HOME");
//        System.setProperty("KETTLE_HOME", file.getAbsolutePath() + File.separator + "molapplugins" + File.separator
//                + "molapplugins");
//        SchemaInfo schemaInfo = new SchemaInfo();
//        schemaInfo.setSrcConUrl("jdbc:oracle:thin:@10.18.51.145:1521:orcl");
//        schemaInfo.setSrcDriverName("oracle.jdbc.OracleDriver");
//        schemaInfo.setSrcUserName("vishal");
//        try
//        {
//            schemaInfo.setSrcPwd(EncryptionUtil.encryptReversible("password"));
//        }
//        catch(Exception e)
//        {
//            
//        }
//        schemaInfo.setSchemaName("Vishal5SecondsTest");
//        schemaInfo.setSchemaPath(file.getAbsolutePath() + File.separator + "test" + File.separator+"resources" + File.separator
//                + "Vishal5SecondsTest1.xml");
//        graphGenerator = new GraphGenerator(schemaInfo);
//        try
//        {
//            graphGenerator.generateGraph();
//            assertTrue(true);
//        }
//        catch(GraphGeneratorException e)
//        {
//            assertTrue(false);
//        }
//    }
//
//    @AfterClass
//    public void tearDown()
//    {
//        graphGenerator = null;
//        if(null != kettleProperty)
//        {
//            System.setProperty("KETTLE_HOME", kettleProperty);
//        }
//
//        File file = new File ("");
//        file = new File (file.getAbsolutePath()+File.separator+"../unibi-solutions");
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
//    public void test_GraphGenerator_Check_Folder_Is_Present_Or_Not_Folder_Will_Be_SchemaName_CubeName()
//    {
//        File file = new File(OUTPUT_LOCATION+File.separator+"Vishal5SecondsTest"+File.separator+"VishalPerfCube");
//        
//        if(file.exists())
//        {
//            assertTrue(true);
//        }
//        else
//        {
//            assertTrue(false);
//        }
//    }
//    
//    @Test
//    public void test_GraphGenerator_Check_OneAGG_OneFACT_Table_Graph_Are_Present_In_Graph_Generator_Output_Folder()
//    {
//        File file = new File(OUTPUT_LOCATION+File.separator+"Vishal5SecondsTest"+File.separator+"VishalPerfCube");
//        
//        if(file.exists())
//        {
//            String[] list = file.list();
//            if(list.length==2)
//            {
//                assertTrue(true);
//            }
//            else
//            {
//                assertTrue(false);
//            }
//        }
//        else
//        {
//            assertTrue(false);
//        }
//    }
//
//    @Test
//    public void test_GraphGenerator_Check_AGGAndFACT_Graph_NameStartWith_FactAndAgg_TableName_AndEndsWith_Ktr_Extension()
//    {
//        List<String> tableNameList = new ArrayList<String>();
//        tableNameList.add("DATA_FACT.ktr".toLowerCase(Locale.getDefault()));
//        tableNameList.add("agg_2_Dev_Year_State_Prot_Temp.ktr".toLowerCase(Locale.getDefault()));
//
//        int counter = 0;
//        File file = new File(OUTPUT_LOCATION+ File.separator + "Vishal5SecondsTest" + File.separator
//                + "VishalPerfCube");
//        if(file.exists())
//        {
//            File[] listFiles = file.listFiles();
//            if(listFiles.length == 2)
//            {
//                for(int i = 0;i < listFiles.length;i++)
//                {
//                    if(tableNameList.contains(listFiles[i].getName().toLowerCase(Locale.getDefault())))
//                    {
//                        counter++;
//                    }
//                }
//            }
//            else
//            {
//                assertTrue(false);
//            }
//        }
//        else
//        {
//            assertTrue(false);
//        }
//
//        if(counter == 2)
//        {
//            assertTrue(true);
//        }
//        else
//        {
//            assertTrue(false);
//        }
//    }
//    
//    @Test
//    public void test_GraphGenerator_WithInvalidSchmaFile_Will_ThorwException()
//    {
//        File file = new File("");
//        SchemaInfo schemaInfo = new SchemaInfo();
//        schemaInfo.setSrcConUrl("jdbc:oracle:thin:@10.18.51.145:1521:orcl");
//        schemaInfo.setSrcDriverName("oracle.jdbc.OracleDriver");
//        schemaInfo.setSrcUserName("vishal");
//        try
//        {
//            schemaInfo.setSrcPwd(EncryptionUtil.encryptReversible("password"));
//        }
//        catch(Exception e)
//        {
//
//        }
//        schemaInfo.setSchemaName("Vishal5SecondsTest");
//        schemaInfo.setSchemaPath(file.getAbsolutePath() + File.separator + "test" + File.separator + "resources"
//                + File.separator + "Vishal5SecondsTest1_en_Invalid.xml");
//        GraphGenerator graphGenerator = new GraphGenerator(schemaInfo);
//
//        try
//        {
//            graphGenerator.generateGraph();
//            assertTrue(false);
//        }
//        catch(RuntimeException e)
//        {
//            assertTrue(true);
//        }
//        catch(Exception e)
//        {
//            assertTrue(false);
//        }
//    }
//    
//    @Test
//    public void test_GraphGenerator_Check_SortTmpFolderIsPresentStoreLocation()
//    {
//        File file = new File(MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL+File.separator+"Vishal5SecondsTest"+File.separator+"VishalPerfCube"+File.separator+"sortrowtmp");
//        
//        if(file.exists())
//        {
//            assertTrue(true);
//        }
//        else
//        {
//            assertTrue(false);
//        }
//        
//    }
}
