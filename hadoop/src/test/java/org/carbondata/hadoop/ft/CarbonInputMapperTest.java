package org.carbondata.hadoop.ft;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.hadoop.CarbonInputFormat;
import org.carbondata.hadoop.CarbonProjection;
import org.carbondata.hadoop.test.util.StoreCreator;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.LiteralExpression;
import org.carbondata.query.expression.conditional.EqualToExpression;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonInputMapperTest extends TestCase {

  @Before public void setUp() throws Exception {
    StoreCreator.createCarbonStore();
  }

  @Test public void testInputFormatMapperReadAllRowsAndColumns() throws Exception {
    /*try {
      String outPath = "target/output";
      runJob(outPath, null, null);
      Assert.assertTrue("Count lines are not matching", countTheLines(outPath) == 1000);
      Assert.assertTrue("Column count are not matching", countTheColumns(outPath) == 7);
    } catch (Exception e) {
      Assert.assertTrue("failed", false);
      e.printStackTrace();
      throw e;
    }*/
  }

  @Test public void testInputFormatMapperReadAllRowsAndFewColumns() throws Exception {
  /*  try {
      String outPath = "target/output2";
      CarbonProjection carbonProjection = new CarbonProjection();
      carbonProjection.addColumn("ID");
      carbonProjection.addColumn("country");
      carbonProjection.addColumn("salary");
      runJob(outPath, carbonProjection, null);

      Assert.assertTrue("Count lines are not matching", countTheLines(outPath) == 1000);
      Assert.assertTrue("Column count are not matching", countTheColumns(outPath) == 3);
    } catch (Exception e) {
      Assert.assertTrue("failed", false);
    } */
  }

  @Test public void testInputFormatMapperReadAllRowsAndFewColumnsWithFilter() throws Exception {
   /* try {
      String outPath = "target/output3";
      CarbonProjection carbonProjection = new CarbonProjection();
      carbonProjection.addColumn("ID");
      carbonProjection.addColumn("country");
      carbonProjection.addColumn("salary");
      Expression expression= new EqualToExpression(new ColumnExpression("country",
          DataType.StringType), new LiteralExpression("france", DataType.StringType));
      runJob(outPath, carbonProjection, expression);
      Assert.assertTrue("Count lines are not matching", countTheLines(outPath) == 101);
      Assert.assertTrue("Column count are not matching", countTheColumns(outPath) == 3);
    } catch (Exception e) {
      Assert.assertTrue("failed", false);
    } */
  }

 /* private int countTheLines(String outPath) throws Exception {
    File file = new File(outPath);
    if (file.exists()) {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      int i = 0;
      while (reader.readLine() != null) {
        i++;
      }
      reader.close();
      return i;
    }
    return 0;
  }

  private int countTheColumns(String outPath) throws Exception {
    File file = new File(outPath);
    if (file.exists()) {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String[] split = reader.readLine().split(",");
      reader.close();
      return split.length;
    }
    return 0;
  } */

  public static class Map extends Mapper<Void, Object[], Void, Text> {

    private BufferedWriter fileWriter;

    public void setup(Context context) throws IOException, InterruptedException {
      String outPath = context.getConfiguration().get("outpath");
      File outFile = new File(outPath);
      try {
        fileWriter = new BufferedWriter(new FileWriter(outFile));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void map(Void key, Object[] value, Context context) throws IOException {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < value.length; i++) {
        builder.append(value[i]).append(",");
      }
      fileWriter.write(builder.toString().substring(0, builder.toString().length() - 1));
      fileWriter.newLine();
    }

    @Override public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      fileWriter.close();
    }
  }

  private void runJob(String outPath, CarbonProjection projection, Expression filter) throws Exception {

    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(CarbonInputMapperTest.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    //    job.setReducerClass(WordCountReducer.class);
    job.setInputFormatClass(CarbonInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    AbsoluteTableIdentifier abs = StoreCreator.getAbsoluteTableIdentifier();
    CarbonInputFormat.setTableToAccess(job.getConfiguration(), abs.getCarbonTableIdentifier());
    if(projection != null) {
      CarbonInputFormat.setColumnProjection(projection, job.getConfiguration());
    }
    if(filter != null) {
      CarbonInputFormat.setFilterPredicates(job.getConfiguration(), filter);
    }
    FileInputFormat.addInputPath(job, new Path(abs.getStorePath()));
    CarbonUtil.deleteFoldersAndFiles(new File(outPath + "1"));
    FileOutputFormat.setOutputPath(job, new Path(outPath + "1"));
    job.getConfiguration().set("outpath", outPath);
    boolean status = job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    new CarbonInputMapperTest().runJob("target/output", null, null);
  }
}
