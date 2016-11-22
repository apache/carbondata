package org.apache.carbondata.tools;

import java.io.IOException;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.util.SchemaReader;

/**
 * This tool prints out schema information of the CarbonData files (by giving args[0] as table path)
 */
public class SchemaTool {

  public static void main(String[] args) {

    if (args.length < 1) {
      printUsage();
      return;
    }

    String tablePath = args[args.length - 1];

    AbsoluteTableIdentifier identifier = AbsoluteTableIdentifier.fromTablePath(tablePath);

    try {
      CarbonTable table = SchemaReader.readCarbonTableFromStore(identifier);
      if (table != null) {
        System.out.println(table);
      } else {
        System.out.println("it is not a valid table path: " + tablePath);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void printUsage() {
    String usage =
        "command line tool to print carbondata files, it prints out schema by default \n" +
        "usage: carbon-ls path\n" +
        "arguments:\n" +
        "\tpath: path to carbondatat files\n";

    System.out.println(usage);
  }

}
