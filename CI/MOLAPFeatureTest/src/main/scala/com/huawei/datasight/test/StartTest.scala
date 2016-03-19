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

package com.huawei.datasight.test

import org.apache.spark.sql.OlapContext
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.SparkContext
import com.huawei.unibi.molap.util.MolapProperties
import collection.JavaConversions._
import org.apache.spark.sql.cubemodel.Partitioner
import scala.io.Source._
import java.text.MessageFormat
import org.apache.spark.sql.CarbonEnv
import java.io.File
import org.apache.spark.sql.hive.HiveContext
import scala.util.control.Exception
import com.huawei.datasight.xml.XmlReportWriter

object StartTest {

  def main(args: Array[String]) {
   val reportWriter= new XmlReportWriter();
    val conf:String = args(0);
    
    val file:File=new File(conf)
    val basePath:String=file.getParentFile.getAbsolutePath
    val prop = new ScalaProp()
    try {
      prop.load(new FileInputStream(file))
    } catch {
      case e: Exception =>
        print(s"unable to load $conf")
    }
    prepareTest(basePath,prop)
    val resultFile:String=MessageFormat.format(prop.getProperty("molap.test.result"),basePath)
    val pw = new java.io.PrintWriter(new File(resultFile))
    var sparkConf = new SparkConf()
    sparkConf.setMaster(prop.getProperty("spark.master.url"))
    sparkConf.set("molap.is.columnar.storage", "true")
    sparkConf.set("spark.sql.dialect", "hiveql")
    sparkConf.set("spark.sql.bigdata.register.dialect", "org.apache.spark.sql.MolapSqlDDLParser")
    sparkConf.set("spark.sql.bigdata.register.strategyRule", "org.apache.spark.sql.hive.CarbonStrategy")
    sparkConf.set("spark.sql.bigdata.initFunction", "org.apache.spark.sql.CarbonEnv")
    sparkConf.set("spark.sql.bigdata.acl.enable", "false") 
    sparkConf.set("spark.sql.bigdata.register.strategy.useFunction", "true")
    val confKeys = prop.keySet.toList.map(_.toString).filter(_.startsWith("sparkconf."))
    confKeys.foreach(x => sparkConf.set(x.stripPrefix("sparkconf."), prop.getProperty(x)))
    sparkConf.setAppName("Functional Test Suite")

    val storePath:String=MessageFormat.format(prop.getPropOrEmpty("spark.store.path"),basePath)
    MolapProperties.getInstance.addProperty("carbon.storelocation", storePath/*prop.get("spark.store.path").toString()*/) 
    val sc = new SparkContext(sparkConf);
    val hc = new HiveContext(sc);
    val warmUpTime = MolapProperties.getInstance().getProperty("molap.spark.warmUpTime", prop.getProperty("molap.spark.warmUpTime"))
    println("Sleeping for millisecs:" + warmUpTime);
    try {
      Thread.sleep(Integer.parseInt(warmUpTime));
    } catch {
      case _ => { println("Wrong value for molap.spark.warmUpTime " + warmUpTime + "Using default Value and proceeding"); Thread.sleep(30000); }
    }


    val partitionCount = prop.getProperty("olap.loadschema.numberOfPartition").toInt
    val cubeName = prop.getProperty("spark.cube.name","test")
    val schemaName = prop.getProperty("spark.schema.name","test")
    val partitionColumn = prop.getProperty("olap.loadschema.PartitionerColumn","imei")
    
    val factPath:String=MessageFormat.format(prop.getProperty("olap.partitionData.fact.path"),basePath)
   
    println("referred variables :--- ")
    
    val createSQL = s"create cube " + cubeName+
                 " dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)"+ 
                "measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' columns= (" + 
                partitionColumn+")"+ 
                "PARTITION_COUNT="+partitionCount+"] )"

    val loadSQl = "LOAD DATA FACT FROM '"+factPath+"' INTO Cube " + cubeName + " partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')"
    
    if(prop.get("olap.loaddata").equals("true"))
    {
       // prepare for test 
   
       val olapKeys = prop.keySet.toList.map(_.toString).filter(_.startsWith("olapcontext."))
       olapKeys.foreach(x => hc.setConf(x.stripPrefix("olapcontext."), prop.getProperty(x)))
//       val schemaPath:String=MessageFormat.format(prop.getProperty("spark.schema.path"),basePath)

       
       hc.sql("show cubes").show
       try {
         println(s"executing>>>>>> Drop cube ")
           hc.sql("Drop cube " + cubeName).show
           println(s"executed>>>>>> Drop cube ")
       } catch {
       case e: Exception => e.printStackTrace()
        println(s"failed>>>>>> Drop cube ")
       }
       
       hc.sql("show cubes").show
       
       println(s"executing>>>>>> createSQL ")
       hc.sql(createSQL).show;
        println(s"executed>>>>>> createSQL ")
        
        println(s"executing>>>>>> loadSQl ")
       hc.sql(loadSQl).show;
         println(s"executed>>>>>> loadSQl")

//       OlapContext.loadSchema(schemaPath, prop.getProperty("olap.loadschema.encrypted") equalsIgnoreCase "true",
//       prop.getProperty("olap.loadschema.aggTablesGen") equalsIgnoreCase "true",
//       Partitioner(prop.getProperty("olap.loadschema.Partitioner"), Seq(prop.getProperty("olap.loadschema.PartitionerColumn")).toArray, prop.getProperty("olap.loadschema.numberOfPartition").toInt, null))(hc)
//  
//       val partitionPath:String=MessageFormat.format(prop.getProperty("spark.partition.path"),basePath)
//       val dimension:String=prop.getPropOrNull("spark.dimension.path")
//       val dimensionPath:String=null
//       if(null!=dimension)
//       {
//         val dimensionPath:String=MessageFormat.format(prop.getPropOrNull("spark.dimension.path"),basePath)  
//       }
//       
//       
//       if(prop.getProperty("olap.partitionData.partition").equalsIgnoreCase("true"))
//       OlapContext.partitionData(prop.getProperty("spark.schema.name"), prop.getProperty("spark.cube.name"),factPath,partitionPath)(hc)
//       
//       OlapContext.loadData(prop.getProperty("spark.schema.name"), prop.getProperty("spark.cube.name"),partitionPath,dimensionPath)(hc)
    }
    
//    hc.sql("""CREATE TABLE Carbon_Test USING org.apache.spark.sql.CarbonSource OPTIONS (cubename "Carbon_Test", storepath s"$basePath")   """).show()
    
     println(s"executing>>>>>> show cubes")
     hc.sql("show cubes").show
     println(s"executed>>>>>> show cubes")
    
    val sqlFilePath:String=MessageFormat.format(prop.getProperty("molap.test.sqlFile"),basePath)
    sqlFilePath.split(",").foreach(readFileAndExec(_, hc, pw,reportWriter))
    pw.close
    reportWriter.flushToResultFile()
    hc.clearCache();
  
    sc.cancelAllJobs();
    sc.stop()
    
  }
  def readFileAndExec(fileName: String, hc: HiveContext, pw: java.io.PrintWriter,reportWriter: XmlReportWriter): Unit = {
   fromFile(fileName).getLines.filterNot(_.trim.startsWith("//")).foreach(_.split(";").filterNot(_.trim.equals("")).foreach ( processCommand(_, hc, pw,reportWriter) ))
  }
	def test(z :String){
	  println(z)
	}
  def processCommand(cmd: String, hc: HiveContext, pw: java.io.PrintWriter,reportWriter: XmlReportWriter): Unit = {
    try {
      println(s"executing>>>>>>$cmd")
      val result=hc.sql(cmd)
              result.show(100)
              println(s"executed>>>>>>$cmd")
      pw.write(s"$cmd  | Success \r\n")
      reportWriter.writePASS("1", s"$cmd")
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        pw.write(s"$cmd | Failed \r\n")
        reportWriter.writeFAIL("1", s"$cmd",ex.getMessage)
      }
    }
  }

  def prepareTest(basePath:String,prop: Properties) {
      // remove the store
//    if(prop.get("olap.loaddata").equals("true"))
//    {
//        val fileName = MessageFormat.format(prop.getProperty("spark.store.path"),basePath);
//        println(s"removing exising store content from $fileName")
//        val f = new File(fileName)
//        delete(f);
//    }
    val resultFile = MessageFormat.format(prop.getProperty("molap.test.result"),basePath);
    println(s"removing exising $resultFile")
    val resultf = new File(resultFile)
    if (resultf.exists())
    	resultf.delete()
  }
  def delete(file:File)
  {
    if(file.isDirectory() && file.listFiles().length > 0 )
    {
      val files=file.listFiles()
      if ( files.length > 0) files.foreach { delete(_) }
      file.delete();
    }
    else
    {
      file.delete();
    }
  }
  
	def filterComments(){
	  	val removedComments = fromFile("test.txt").getLines.reduce(
		  (x,y) =>
		  y match {
		    case y  if y.trim().startsWith("//") => x 
		    case _ => x +"\n"+y
		  }
		)
	}
}

class ScalaProp extends java.util.Properties{
  
	private def get(key : String ) : Option[String]={
	  val value =getProperty(key)
	  if(value!=null){
	    Some(value)
	  }else{
	    None
	  }
	}
	def getPropOrEmpty(key : String ) : String={
	 get(key) match {
	   case Some(v) => v
	   case None => ""
	 }
  }
  def getPropOrNull(key:String):String={
      get(key) match {
          case Some(v) => v
          case None => null
      }
  }
}