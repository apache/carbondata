package test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class CreateBitMapData1 {

  public CreateBitMapData1() {

  }

  public static void main(String[] args) {

    FileOutputStream out = null;

    FileOutputStream outSTr = null;

    BufferedOutputStream Buff = null;

    FileWriter fw = null;

    int count = 1000;

    try {

      outSTr = new FileOutputStream(new File("./src/main/resources/bitmaptest1.csv"));

      Buff = new BufferedOutputStream(outSTr);

      long begin0 = System.currentTimeMillis();
      Buff.write("ID,country0,date,country,name,phonetype,country1,serialname,salary,country2\n"
          .getBytes());

      int idcount = 1500000;
      int datecount = 30;
      int countrycount = 9;
      // int namecount =5000000;
      int phonetypecount = 10000;
      int serialnamecount = 50000;
      // int salarycount = 200000;
      Map<Integer, String> countryMap = new HashMap<Integer, String>();
      countryMap.put(1, "usa");
      countryMap.put(2, "uk");
      countryMap.put(3, "china");
      countryMap.put(4, "indian");
      countryMap.put(5, "japan");
      countryMap.put(6, "korea");
      countryMap.put(7, "russia");
      countryMap.put(8, "poland");
      countryMap.put(0, "canada");

      StringBuilder sb = null;
      int id;
      for (int i = idcount; i > 0; i--) {

        sb = new StringBuilder();
        id = 4000000 + i;
        sb.append(id).append(",");// id
        sb.append(i == 1 ? "france" : countryMap.get((i + 1) % countrycount)).append(",");
        sb.append("2015/8/" + (i % datecount + 1)).append(",");
        sb.append(i == 1 ? "france" : countryMap.get(i % countrycount)).append(",");
        sb.append("name" + (1600000 - i)).append(",");// name
        sb.append("phone" + i % phonetypecount).append(",");
        sb.append(i == 1 ? "france" : countryMap.get((i + 2) % countrycount)).append(",");
        sb.append("serialname" + (100000 + i % serialnamecount)).append(",");// serialname
        sb.append(i + 500000).append(',');
        sb.append(i == 1 ? "france" : countryMap.get((i + 3) % countrycount)).append('\n');
        // System.out.println("sb.toString():" + sb.toString());
        Buff.write(sb.toString().getBytes());
        if (id == 4000001) {
          System.out.println("sb.toString():" + sb.toString());
        }
      }

      Buff.flush();

      Buff.close();
      System.out.println("sb.toString():" + sb.toString());
      long end0 = System.currentTimeMillis();

      System.out.println("BufferedOutputStream execute time:" + (end0 - begin0) + " ms");

    } catch (Exception e) {

      e.printStackTrace();

    }

    finally {

      try {

        // fw.close();

        Buff.close();

        outSTr.close();

        // out.close();

      } catch (Exception e) {

        e.printStackTrace();

      }

    }

  }

}