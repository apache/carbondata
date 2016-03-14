/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 1997
 * =====================================
 *
 */
package com.huawei.datasight.molap.spark.util;


/**
 * 
 * @author K00900207
 *
 */
public class MultiPartitionTest
{

//    /**
//     * 
//     * @param args
//     * 
//     */
//    public static void main(String[] args)
//    {
//        String schemaOriginalPath = "D:/f/SVN/TRP/2014/SparkOLAP/PCC_Java.xml";
//        Schema schema = MolapSchemaParser.loadXML(schemaOriginalPath);
//        
//        MolapProperties.getInstance().addProperty("molap.storelocation", "D:/f/SVN/TRP/2014/SparkOLAP/sparkolaptest/PCC");
//
//        String originalSchemaName = schema.name;
//        String originalCubeName = schema.cubes[0].name;
//        schema.name = originalSchemaName+"_0";
//        schema.cubes[0].name = originalCubeName +"_0";
//        
//        String schemaPartition1 = schema.toXML();
//        
//        PropertyList makeConnectString = MolapQueryUtil.makeConnectString(schemaOriginalPath);
//        makeConnectString.put(RolapConnectionProperties.CatalogContent.name(), schemaPartition1);
//        DataSource molapDataSource = new MolapDataSourceFactory().getMolapDataSource(makeConnectString);
//        Connection connection = DriverManager.getConnection(makeConnectString, null,molapDataSource);
//        
//        schema.name = originalSchemaName+"_1";
//        schema.cubes[0].name = originalCubeName +"_1";
//        String schemaPartition2 = schema.toXML();
//        makeConnectString = MolapQueryUtil.makeConnectString(schemaOriginalPath);
//        makeConnectString.put(RolapConnectionProperties.CatalogContent.name(), schemaPartition2);
//        molapDataSource = new MolapDataSourceFactory().getMolapDataSource(makeConnectString);
//        connection = DriverManager.getConnection(makeConnectString, null,molapDataSource);
//        
//        InMemoryCubeStore.getInstance().getCubeNames();
//        System.out.println(Arrays.toString(InMemoryCubeStore.getInstance().getCubeNames()));
//
//        
//        MolapQueryInternalExecutorImpl executorImpl = new MolapQueryInternalExecutorImpl();
//        
//        MolapQuery molapQuery = createMolapQuery();
//        MolapResultStreamHolder resultStreamHolder1 = executorImpl.execute(molapQuery, "PCC_0", "ODM_0");
//        
//        MolapResultStreamHolder resultStreamHolder2 = executorImpl.execute(molapQuery, "PCC_1", "ODM_1");
//        
//        printResult(resultStreamHolder1.getResultStream());
//        printResult(resultStreamHolder2.getResultStream());
//    }
//    
//    private static MolapQuery createMolapQuery()
//    {
//        MolapQueryImpl molapQuery = new MolapQueryImpl();
//        
//        MolapDimensionLevel level1 = new MolapDimensionLevel("Time", "Time", "Year");
//        MolapDimensionLevelFilter filter = new MolapDimensionLevelFilter();
//        List<Object> times = new ArrayList<Object>();
//        times.add("[2014]");
//        filter.setIncludeFilter(times);
//        molapQuery.addDimensionLevel(level1, filter, SortType.ASC, AxisType.ROW);
//        MolapMeasure measure2 = new MolapMeasure("L4_UL_THROUGHPUT");
//        
//        
//        molapQuery.addMeasure(measure2,null, SortType.NONE);
//        
//        return molapQuery;
//    }
//    
//    private static void printResult( MolapResultStream result ) {
//        List<MolapTuple> columnTuples = result.getColumnTuples();
//        
//        while(result.hasNext())
//        {
//            //Get the result chunk.
//            MolapResultChunk resultChunk = result.getResult();
//            List<MolapTuple> rowTuples = resultChunk.getRowTuples();
//            
//              for (int i = 0; i < rowTuples.size(); i++)
//              {
//                    MolapTuple row = rowTuples.get(i);
//                    
//                    
//                    for (Object member : row.getTuple())
//                    {
//                        System.out.print(member);
//                        System.out.print(" ");
//                    }
//                    if(columnTuples.size() > 0)
//                    {
//                        for (int j = 0; j < columnTuples.size(); j++)
//                        {
//                            System.out.print(Arrays.toString(columnTuples.get(j).getTuple())+" | "+resultChunk.getCell(j , i));
//                            System.out.print(" ");
//                        }
//                    }
//                    else
//                    {
//                        System.out.print(resultChunk.getCell(0 , i));
//                        System.out.print(" ");
//                    }
//                    System.out.println();
//                }
//              System.out.println(resultChunk.getRowTuples().size());
//        }
//    }

}
