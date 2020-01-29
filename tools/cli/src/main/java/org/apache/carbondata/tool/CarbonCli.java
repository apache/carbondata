/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.tool;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * CarbonCli tool, which can be run as a standalone java application.
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class CarbonCli {

  static class OptionsHolder {
    static Options instance = buildOptions();
  }

  private static Options buildOptions() {
    Option help = new Option("h", "help", false, "print this message");
    Option path = OptionBuilder.withArgName("path")
        .hasArg()
        .withDescription("the path which contains carbondata files, nested folder is supported")
        .withLongOpt("path")
        .create("p");
    Option file = OptionBuilder.withArgName("file")
        .hasArg()
        .withDescription("the carbondata file path")
        .withLongOpt("file")
        .create("f");

    Option command = OptionBuilder
        .withArgName("command name")
        .hasArg()
        .withDescription("command to execute, supported commands are: summary, benchmark")
        .isRequired(true)
        .create("cmd");

    Option all = new Option("a", "all", false, "print all information");
    Option schema = new Option("s", "schema", false, "print the schema");
    Option segment = new Option("m", "showSegment", false, "print segment information");
    Option tblProperties = new Option("t", "tblProperties", false, "print table properties");
    Option columnMeta = new Option("k", "columnChunkMeta", false, "print column chunk meta");
    Option columnName = OptionBuilder
        .withArgName("column name")
        .hasArg()
        .withDescription("column to print statistics")
        .withLongOpt("column")
        .create("c");
    Option columns = OptionBuilder
        .withDescription("print statistics for all columns")
        .withLongOpt("columns")
        .create("C");

    Option blockletDetail = OptionBuilder.withArgName("limitSize").hasOptionalArg()
        .withDescription("print blocklet size detail").withLongOpt("limitSize")
        .create("b");

    Option blockLevelDetail = OptionBuilder.withArgName("blockDetail").hasArg()
        .withDescription("print block details").withLongOpt("blockDetail")
        .create("B");

    Option version = new Option("v", "version", false, "print version details of carbondata file");
    Options options = new Options();
    options.addOption(help);
    options.addOption(path);
    options.addOption(file);
    options.addOption(command);
    options.addOption(all);
    options.addOption(schema);
    options.addOption(segment);
    options.addOption(tblProperties);
    options.addOption(blockletDetail);
    options.addOption(columnMeta);
    options.addOption(columnName);
    options.addOption(columns);
    options.addOption(version);
    options.addOption(blockLevelDetail);
    return options;
  }

  public static void main(String[] args) {
    run(args, System.out);
  }

  /**
   * adapter to run CLI and print to stream
   * @param args input arguments
   * @param out output stream
   */
  public static void run(String[] args, PrintStream out) {
    ArrayList<String> outputs = new ArrayList<String>();
    run(args, outputs, true);
    for (String line: outputs) {
      out.println(line);
    }
  }

  /**
   * run CLI and fill result into array
   * @param args input arguments
   * @param outPuts array for filling result
   * @param isPrintInConsole flag to decide whether to print error in console or return the list
   */
  public static void run(String[] args, ArrayList<String> outPuts, boolean isPrintInConsole) {
    Options options = OptionsHolder.instance;
    CommandLineParser parser = new PosixParser();

    CommandLine line = null;
    try {
      line = parser.parse(options, args);
    } catch (ParseException ex) {
      if (isPrintInConsole) {
        outPuts.add("Parsing failed. Reason: " + ex.getMessage());
        return;
      } else {
        throw new RuntimeException("Parsing failed. Reason: " + ex.getMessage(), ex);
      }
    }

    if (line.hasOption("h")) {
      outPuts.add(collectHelpInfo(options));
      return;
    }

    String path = "";
    if (line.hasOption("p")) {
      path = line.getOptionValue("path");
    }
    outPuts.add("Input Folder: " + path);

    String cmd = line.getOptionValue("cmd");
    Command command;
    if (cmd.equalsIgnoreCase("summary")) {
      command = new DataSummary(path, outPuts);
    } else if (cmd.equalsIgnoreCase("benchmark")) {
      command = new ScanBenchmark(path, outPuts);
    } else if (cmd.equalsIgnoreCase("sort_columns")) {
      if (line.hasOption("p")) {
        new FileCollector(outPuts).collectSortColumns(line.getOptionValue("p"));
      }
      return;
    } else {
      outPuts.add("command " + cmd + " is not supported");
      outPuts.add(collectHelpInfo(options));
      return;
    }

    try {
      command.run(line);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String collectHelpInfo(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    formatter.printHelp(printWriter, formatter.getWidth(), "CarbonCli", null, options,
        formatter.getLeftPadding(), formatter.getDescPadding(), null, false);
    printWriter.flush();
    return stringWriter.toString();
  }

}
