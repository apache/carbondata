/**
 * 
 */
package com.huawei.datasight.molap.spark.util;

/*import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.huawei.datasight.molap.load.MolapLoadModel;
import com.huawei.datasight.spark.SparkQueryExecutor;
import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.MolapQuery.AxisType;
import com.huawei.unibi.molap.query.MolapQuery.SortType;
import com.huawei.unibi.molap.query.impl.MolapQueryImpl;
import com.huawei.unibi.molap.query.metadata.DSLTransformation;
import com.huawei.unibi.molap.query.metadata.Dataset;
import com.huawei.unibi.molap.query.metadata.Dataset.DatasetType;
import com.huawei.unibi.molap.query.metadata.MolapCalculatedMeasure;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevel;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;
import com.huawei.unibi.molap.query.metadata.MolapMeasure;
import com.huawei.unibi.molap.query.metadata.MolapMeasureFilter;
import com.huawei.unibi.molap.query.metadata.MolapMeasureFilter.FilterType;
import com.huawei.unibi.molap.util.MolapProperties;
*/
/**
 * @author R00900208
 *
 */
public class SparkMolapTest {

	/**
	 * @param args
	 */
	/*public static void main(String[] args) {
//		executeSteelWheels();
//		executeHQ();
		executePcc();
//		executeHQSQL();
//		loadPCC();
	}*/

	/*private static void executeSteelWheels() {
		SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml","F:/TRPSVN/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","","G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3/","/opt/ravi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","/opt/spark-1.0.0-rc3/","/opt/ravi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
	    
//	    SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","G:/spark-1.0.0-rc3","D:/f/SVN/TRP/2014/SparkOLAPRavi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
//	    SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","D:/f/SVN/TRP/2014/SparkOLAPRavi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","../store");
	//	exec.init(null);
		
		MolapQueryImpl molapQuery = new MolapQueryImpl();
		  
		   MolapDimensionLevel level1 = new MolapDimensionLevel("Markets", "Markets", "Territory");
//		   MolapDimensionLevel level1 = new MolapDimensionLevel("Time", "Time", "Year");
		   MolapDimensionLevelFilter filter1 = new MolapDimensionLevelFilter();
		   List<String> includeexcludeFilter1 = new ArrayList<String>();
//	       includeexcludeFilter1.contains("1");
		   includeexcludeFilter1.add("AP");
//		   includeexcludeFilter1.add("EMEA");
		   
		   filter1.setDoesNotContainsFilter(includeexcludeFilter1);
//		   filter1.set(includeexcludeFilter1);
		   molapQuery.addDimensionLevel(level1, filter1, SortType.ASC, AxisType.ROW);
		   
		   MolapDimensionLevel level2 = new MolapDimensionLevel("Markets", "Markets", "Country");
		   MolapDimensionLevelFilter filter2 = new MolapDimensionLevelFilter();
		   List<Object> includeexcludeFilter2 = new ArrayList<Object>();
		   includeexcludeFilter2.add("New Zealand");
		   includeexcludeFilter2.add("Austria");
//		   filter2.setIncludeFilter(includeexcludeFilter2);
		   molapQuery.addDimensionLevel(level2, filter2, SortType.NONE, AxisType.ROW);
		   
		   
		   MolapDimensionLevel level3 = new MolapDimensionLevel("Markets", "Markets", "State Province");

//		   molapQuery.addDimensionLevel(level3, null, SortType.NONE, AxisType.ROW);
		   
		   MolapDimensionLevel level4 = new MolapDimensionLevel("Time", "Time", "Years");
		   
		   molapQuery.addDimensionLevel(level4, null, SortType.NONE, AxisType.ROW);
		   
		   MolapMeasure measure2 = new MolapMeasure("Quantity");
		   molapQuery.addMeasure(measure2, null, SortType.NONE);
		   
		   MolapCalculatedMeasure measure3 = new MolapCalculatedMeasure("ctQuantity",null);
		   measure3.setGroupDimensionLevel(level3);
		   molapQuery.addMeasure(measure3, null, SortType.NONE);
		   
		   molapQuery.addTopCount(level2, measure2, 1);
		   
		   String tr = "def segmentRange(row : Row):Any= \n"+
				   "{ \n"+
				   "var value = row.getDouble(0); \n"+
				   "if( value >= 0 &&  value < 1000 ) \"0-1K Range\" \n"+
				   "else if  ( value >= 1000 &&  value < 10000 ) \"1K-10K Range\" \n"+
				   "else \"Other\" \n"+
				   "} \n"+
				   "curr.addColumn(SqlUdf(segmentRange,\"curr.Quantity\".attr) as 'Ranges)\n";
		   
		   List<DSLTransformation> trans = new ArrayList<DSLTransformation>();
		   trans.add(new DSLTransformation(null, tr, "Ranges", true));
//		   trans.add(new DSLTransformation(null, "curr.addColumn(SqlUdf( _.getDouble(0)+10 ,'quantity) as 'qq1)", "dd", true));
		   molapQuery.getExtraProperties().put(MolapQuery.TRANSFORMATIONS, trans);
		   ArrayList<Dataset> datasets = new ArrayList<Dataset>();
		   ArrayList<Dataset.Column> columns = new ArrayList<Dataset.Column>();
		   columns.add(new Dataset.Column("column1", "String", ""));
		   columns.add(new Dataset.Column("column2", "String", ""));
		   Dataset dataset1 = new Dataset(DatasetType.REPORT_DATA_EXPORT, "ds1", columns);
		   datasets.add(dataset1);

		   molapQuery.getExtraProperties().put("DATA_SETS", datasets);
		   

		
		//System.out.println(exec.execute(molapQuery));
	}
	
	private static void executePcc() {
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://9.91.8.157:7077","G:/spark-1.0.0-rc3","/home/smu/PCC_Java.xml","PCC","ODM","hdfs://9.91.8.157:9000/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","/home/smu/PCC_Java.xml","PCC","ODM","/home/smu/store"); //9.91.8.157
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","","G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
		SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","G:/mavenlib/PCC_Java.xml","hdfs://10.18.51.225:54310/opt/kk/perftest/hdfs1dayload");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3/","/opt/ravi/PCC_Java.xml","PCC","ODM","hdfs://master:54310/opt/ravi/perfstore");
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","/opt/spark-1.0.0-rc3/","/opt/ravi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
	    
//	    SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","G:/spark-1.0.0-rc3","D:/f/SVN/TRP/2014/SparkOLAPRavi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
		//exec.init(null);
		
		MolapQueryImpl molapQuery = new MolapQueryImpl();
		  
//		   MolapDimensionLevel level1 = new MolapDimensionLevel("Time", "Time", "Year");
		   MolapDimensionLevel level1 = new MolapDimensionLevel("RAT", "RAT", "RAT");
//		   MolapDimensionLevel level1 = new MolapDimensionLevel("Time", "Time", "Year");
		   MolapDimensionLevelFilter filter1 = new MolapDimensionLevelFilter();
		   List<Object> includeexcludeFilter1 = new ArrayList<Object>();
//	       includeexcludeFilter1.contains("1");
//		   includeexcludeFilter1.add("AP");
		   includeexcludeFilter1.add("1");
		   
//		   filter1.setDoesNotContainsFilter(includeexcludeFilter1);
		   filter1.setIncludeFilter(includeexcludeFilter1);
//		   molapQuery.addDimensionLevel(level1, filter1, SortType.ASC, AxisType.ROW);
//		   molapQuery.addSlice(level1, filter1);
//		   MolapDimensionLevel level2 = new MolapDimensionLevel("Time", "Time", "Month");
//		   MolapDimensionLevelFilter filter2 = new MolapDimensionLevelFilter();
//		   List<Object> includeexcludeFilter2 = new ArrayList<Object>();
//		   includeexcludeFilter2.add("New Zealand");
//		   includeexcludeFilter2.add("Austria");
////		   filter2.setIncludeFilter(includeexcludeFilter2);
//		   molapQuery.addDimensionLevel(level2, null, SortType.NONE, AxisType.ROW);
//		   
//		   MolapDimensionLevel level4 = new MolapDimensionLevel("Time", "Time", "Hour");
//		   MolapDimensionLevelFilter filter4 = new MolapDimensionLevelFilter();
//		   List<Object> includeexcludeFilter4 = new ArrayList<Object>();
////		   filter2.setIncludeFilter(includeexcludeFilter2);
//		   molapQuery.addDimensionLevel(level4, null, SortType.NONE, AxisType.ROW);		   
//		   
//		   
		   MolapDimensionLevel level3 = new MolapDimensionLevel("Msisdn", "Msisdn", "Msisdn");

//		   molapQuery.addDimensionLevel(level3, null, SortType.NONE, AxisType.ROW);
		   
//		   MolapDimensionLevel level5 = new MolapDimensionLevel("Terminal", "TerminalModel", "Tac");
//
//		   molapQuery.addDimensionLevel(level3, null, SortType.NONE, AxisType.ROW);
		   
		   
		   
		   MolapMeasure measure2 = new MolapMeasure("L4_UL_THROUGHPUT");
		   List<MolapMeasureFilter> msrFilter = new ArrayList<MolapMeasureFilter>();
		   MolapMeasureFilter mf = new MolapMeasureFilter(300000, FilterType.GREATER_THAN);
//		   msrFilter.add(mf);
		   molapQuery.addMeasure(measure2, null, SortType.NONE);
		   
		   
		  
		   
		   List<DSLTransformation> trans = new ArrayList<DSLTransformation>();
		   trans.add(new DSLTransformation(null, "curr.where('L4_UL_THROUGHPUT)((a:com.huawei.unibi.molap.engine.aggregator.impl.SumAggregator) =>  a.getValue >= 6)", "dd", false));
//		   molapQuery.getExtraProperties().put(MolapQuery.TRANSFORMATIONS, trans);
//		   molapQuery.getExtraProperties().put(MolapQuery.DATA_SET_PATH, "D:/ddd.csv");
		   

	        long s = System.currentTimeMillis();
	        System.out.println("***********BEFORE EXCUTE EXEC********************");
		//System.out.println(exec.execute(molapQuery));
//		exec.execute(molapQuery);
		System.out.println("******************************* " +(System.currentTimeMillis()-s));
	}	
	
	private static void executeHQ() {
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","F:\\wagfei\\molapdata\\SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","","G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3/","/opt/ravi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","/opt/spark-1.0.0-rc3/","/opt/ravi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
	    
//	    SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","G:/spark-1.0.0-rc3","D:/f/SVN/TRP/2014/SparkOLAPRavi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","hdfs://master:54310/opt/ravi/store");
//	    SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","D:/f/SVN/TRP/2014/SparkOLAPRavi/steelwheels.molap.xml","MOLAPSteelWheels","SteelWheelsSales","../store");
		//exec.init(null);
		
        MolapQueryImpl molapQuery = new MolapQueryImpl();

        MolapDimensionLevel level1 = new MolapDimensionLevel("TERMINAL_DIM", "TERMINAL_HIER", "TERMINAL");

        MolapDimensionLevelFilter filter1 = new MolapDimensionLevelFilter();
        List includeexcludeFilter1 = new ArrayList();

       includeexcludeFilter1.add("MOTOROLA");
       filter1.setIncludeFilter(includeexcludeFilter1);

        molapQuery.addDimensionLevel(level1, filter1, SortType.ASC, AxisType.ROW);

        MolapMeasure measure2 = new MolapMeasure("L4_UL_THROUGHPUT");
        molapQuery.addMeasure(measure2, null, SortType.NONE);


        List trans = new ArrayList();
//        molapQuery.getExtraProperties().put("TRANSFORMATIONS", trans);
        //molapQuery.getExtraProperties().put("DATA_SET_PATH", "/home/opt/ddd.csv");
        long s = System.currentTimeMillis();
        System.out.println("***********BEFORE EXCUTE EXEC********************");
        System.out.println(exec.execute(molapQuery));
        System.out.println("******************************* " +(System.currentTimeMillis()-s));

	}	
	
	private static void executeHQSQL() {
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","G:/spark-1.0.0-rc3","F:\\wagfei\\molapdata\\SchemaYM.xml","Schema5D","Cube5D","F:\\wagfei\\molapdata/store");
		SparkQueryExecutor exec = new SparkQueryExecutor("spark://9.91.8.157:7077","G:/spark-1.0.0-rc3","/home/smu/PCC_Java.xml","/home/smu/store"); //9.91.8.157
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
	//	exec.init(null);
//		System.out.println(exec.execute("All cache table ravi as select TERMINAL,sum(L4_UL_THROUGHPUT) from Cube5D group by TERMINAL"));
	//	System.out.println(exec.execute("select Msisdn,sum(L4_DW_THROUGHPUT) from ODM group by Msisdn"));
	}
	
	private static void loadPCC() {
		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","G:/spark-1.0.0-rc3","G:/mavenlib/PCC_Java.xml","F:/TRPSVN/Molap/unibi-solutions/system/molap/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://9.91.8.157:7077","G:/spark-1.0.0-rc3","/home/smu/PCC_Java.xml","PCC","ODM","/home/smu/store"); //9.91.8.157
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
		
		Properties properties = loadProperties();
		
		String schemaName = properties.getProperty("dataloader.schema.name", "PCC");
		String cubeName = properties.getProperty("dataloader.cube.name", "ODM");
		String schemaPath = properties.getProperty("dataloader.schema.path", "/opt/ravi/PCC_Java.xml");
		String dimFolderPah = properties.getProperty("dataloader.dimsdata.path"); //, "/opt/ravi/NewDims"
		String factPath = properties.getProperty("dataloader.factdata.path", "/hadoopmnt/sparkolapdemo/smartpccsimulator/ModifiedSimulatorData/06-09-2014");
		String factTable = properties.getProperty("dataloader.facttable.name", "SDR_PCC_USER_ALL_INFO_1DAY_16085");
		String storepath = properties.getProperty("dataloader.molap.storelocation :", "/opt/ravi/storera");
		
		System.out.println("********** Properties Used ***************");
		System.out.println("dataloader.schema.name :" + schemaName);
		System.out.println("dataloader.cube.name :" + cubeName);
		System.out.println("dataloader.schema.path :" + schemaPath);
		System.out.println("dataloader.dimsdata.path :" + dimFolderPah);
		System.out.println("dataloader.factdata.path :" + factPath);
		System.out.println("dataloader.facttable.name :" + factTable);
		System.out.println("dataloader.molap.storelocation :" + storepath);
		
		MolapProperties.getInstance().addProperty("molap.storelocation", storepath);
		  
		MolapLoadModel model = new MolapLoadModel();
		model.setSchemaPath(schemaPath);
		model.setSchemaName(schemaName);
		model.setCubeName(cubeName);
		if(dimFolderPah !=null)
		{
		    model.setDimFolderPath(dimFolderPah);
		}
		
		model.setFactFilePath(factPath);
		model.setTableName(factTable);
		
		
//		model.setDbPwd("sampledata");
//		model.setDbUserName("sampledata");
//		model.setDimFolderPath("G:/mavenlib/NewDims");
//		model.setDriverClass("com.mysql.jdbc.Driver");
//		model.setFactFilePath("G:/mavenlib");
//		model.setJdbcUrl("jdbc:mysql://10.18.51.240:3999/sampledata");
//		model.setSchemaPath("G:/mavenlib/PCC_Java.xml");
//		exec.init();
//		exec.initLoader(model);
//		System.out.println(exec.execute("All cache table ravi as select TERMINAL,sum(L4_UL_THROUGHPUT) from Cube5D group by TERMINAL"));
//		System.out.println(exec.execute("select Msisdn,sum(L4_DW_THROUGHPUT) from ODM group by Msisdn"));
	}
	
	 *//**
     * 
     * Read the properties from CSVFilePartitioner.properties
     *//*
    private static Properties loadProperties()
    {
        Properties properties = new Properties();

        File file = new File("DataLoader.properties");
        FileInputStream fis = null;
        try
        {
            if(file.exists())
            {
                fis = new FileInputStream(file);

                properties.load(fis);
            }
            else
            {
               System.out.println("Properties file not found @" + file.getAbsolutePath());
            }
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if(null != fis)
            {
                try
                {
                    fis.close();
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        
        return properties;

    }
*/
}
