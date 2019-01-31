package org.apache.carbondata.sdk.file;

import junit.framework.TestCase;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ImageTest extends TestCase {

  @Test
  public void testBinaryWithFilter() throws IOException, InvalidLoadOptionException, InterruptedException {
    String imagePath = "./src/main/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 1;
    String path = "./target/binary";
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        char[] hexValue = null;
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
          hexValue = Hex.encodeHex(originBinary);
        }
        // write data
        writer.write(new String[]{"robot" + (i % 10), String.valueOf(i), String.valueOf(hexValue)});
        bis.close();
      }
      writer.close();
    }

    // Read data with filter
    EqualToExpression equalToExpression = new EqualToExpression(
        new ColumnExpression("name", DataTypes.STRING),
        new LiteralExpression("robot0", DataTypes.STRING));

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .filter(equalToExpression)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[2];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);

      // validate output binary data and origin binary data
      assert (originBinary.length == outputBinary.length);
      for (int j = 0; j < originBinary.length; j++) {
        assert (originBinary[j] == outputBinary[j]);
      }

      // save image, user can compare the save image and original image
      String destString = "./target/binary/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testBinaryWithoutFilter() throws IOException, InvalidLoadOptionException, InterruptedException {
    String imagePath = "./src/main/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 10;
    String path = "./target/binary";
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        char[] hexValue = null;
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
          hexValue = Hex.encodeHex(originBinary);
        }
        // write data
        writer.write(new String[]{"robot" + (i % 10), String.valueOf(i), String.valueOf(hexValue)});
        bis.close();
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[2];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);

      // validate output binary data and origin binary data
      assert (originBinary.length == outputBinary.length);
      for (int j = 0; j < originBinary.length; j++) {
        assert (originBinary[j] == outputBinary[j]);
      }

      // save image, user can compare the save image and original image
      String destString = "./target/binary/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testBinaryWithManyImages() throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 1;
    String path = "./target/flowers";
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    String imageFolder = "./src/main/resources/image/flowers";

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .build();
      File file = new File(imageFolder);
      File[] files = file.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith(".jpg");
        }
      });

      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files[i]));
          char[] hexValue = null;
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
            hexValue = Hex.encodeHex(originBinary);
          }

          String txtFileName = files[i].getCanonicalPath().split(".jpg")[0] + ".txt";
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files[i].getCanonicalPath());
          writer.write(new String[]{String.valueOf(i), files[i].getCanonicalPath(), String.valueOf(hexValue),
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder()
        .withFolder(path)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[4];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);

      // save image, user can compare the save image and original image
      String destString = "./target/flowers/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testBinaryWithProjectionAndFileLists() throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 2;
    String path = "./target/flowers";
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    String imageFolder = "./src/main/resources/image/flowers";

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .build();
      File file = new File(imageFolder);
      File[] files = file.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith(".jpg");
        }
      });

      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files[i]));
          char[] hexValue = null;
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
            hexValue = Hex.encodeHex(originBinary);
          }

          String txtFileName = files[i].getCanonicalPath().split(".jpg")[0] + ".txt";
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files[i].getCanonicalPath());
          writer.write(new String[]{String.valueOf(i), files[i].getCanonicalPath(), String.valueOf(hexValue),
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    File carbonDataFile = new File(path);
    File[] carbonDataFiles = carbonDataFile.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name == null) {
          return false;
        }
        return name.endsWith(CarbonTablePath.CARBON_DATA_EXT);
      }
    });

    System.out.println(carbonDataFiles[0]);
    List fileLists = new ArrayList();
    fileLists.add(carbonDataFiles[0]);
    fileLists.add(carbonDataFiles[1]);

    Schema schema = CarbonSchemaReader.readSchema(carbonDataFiles[0].getAbsolutePath()).asOriginOrder();
    List projectionLists = new ArrayList();
    projectionLists.add((schema.getFields())[1].getFieldName());
    projectionLists.add((schema.getFields())[2].getFieldName());

    CarbonReader reader = CarbonReader
        .builder()
        .withFileLists(fileLists)
        .projection(projectionLists)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      assertEquals(2, row.length);
      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);

      // save image, user can compare the save image and original image
      String destString = "./target/flowers/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    assert (6 == i);
    System.out.println("\nFinished");
    reader.close();

    CarbonReader reader2 = CarbonReader
        .builder()
        .withFile(carbonDataFiles[0].toString())
        .build();

    System.out.println("\nData2:");
    i = 0;
    while (i < 20 && reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();
      assertEquals(5, row.length);

      assert (null != row[0].toString());
      assert (null != row[0].toString());
      assert (null != row[0].toString());
      assert (((int) row[3]) > -1);
      byte[] outputBinary = (byte[]) row[4];

      String txt = row[2].toString();
      System.out.println(row[0] + " " + row[1] +
          " image size:" + outputBinary.length + " txt size:" + txt.length());

      // save image, user can compare the save image and original image
      String destString = "./target/flowers/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    assert (3 == i);
    System.out.println("\nFinished");
    reader2.close();
  }

  @Test
  public void testWithoutTablePath() throws IOException, InterruptedException {
    try {
      CarbonReader
          .builder()
          .build();
      assert (false);
    } catch (IllegalArgumentException e) {
      assert (e.getMessage().equalsIgnoreCase("Please set table path first."));
      assert (true);
    }
  }

  @Test
  public void testWriteWithByteArrayDataType() throws IOException, InvalidLoadOptionException, InterruptedException {
    String imagePath = "./src/main/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 10;
    String path = "./target/binary";
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
        }
        // write data
        writer.write(new Object[]{"robot" + (i % 10), i, originBinary});
        bis.close();
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[2];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);

      // validate output binary data and origin binary data
      assert (originBinary.length == outputBinary.length);
      for (int j = 0; j < originBinary.length; j++) {
        assert (originBinary[j] == outputBinary[j]);
      }

      // save image, user can compare the save image and original image
      String destString = "./target/binary/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesTxt()
      throws InvalidLoadOptionException, InterruptedException, IOException {
    String sourceImageFolder = "./src/main/resources/image/flowers";
    String outputPath = "./target/flowers";
    String preDestPath = "./target/flowers/image";
    String sufAnnotation = ".txt";
    writeAndRead(sourceImageFolder, outputPath, preDestPath, sufAnnotation);
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesXml()
      throws InvalidLoadOptionException, InterruptedException, IOException {
    String sourceImageFolder = "./src/main/resources/image/voc";
    String outputPath = "./target/voc";
    String preDestPath = "./target/voc/image";
    String sufAnnotation = ".xml";
    writeAndRead(sourceImageFolder, outputPath, preDestPath, sufAnnotation);
  }

  public void writeAndRead(String sourceImageFolder, String outputPath, String preDestPath, String sufAnnotation)
      throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 1;
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(outputPath)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example")
          .build();
      File file = new File(sourceImageFolder);
      File[] files = file.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith(".jpg");
        }
      });

      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files[i]));
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
          }

          String txtFileName = files[i].getCanonicalPath().split(".jpg")[0] + sufAnnotation;
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files[i].getCanonicalPath());
          writer.write(new Object[]{i, files[i].getCanonicalPath(), originBinary,
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder()
        .withFolder(outputPath)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[4];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);

      // save image, user can compare the save image and original image
      String destString = preDestPath + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }
}
