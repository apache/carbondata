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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.memory.MemoryException;

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

  private static Options buildOptions() {
    Option help = new Option("h", "help", false,"print this message");
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

    Option all = new Option("a", "all",false, "print all information");
    Option schema = new Option("s", "schema",false, "print the schema");
    Option segment = new Option("m", "showSegment", false, "print segment information");
    Option tblProperties = new Option("t", "tblProperties", false, "print table properties");
    Option detail = new Option("b", "blocklet", false, "print blocklet size detail");
    Option columnName = OptionBuilder
        .withArgName("column name")
        .hasArg()
        .withDescription("column to print statistics")
        .withLongOpt("column")
        .create("c");

    Options options = new Options();
    options.addOption(help);
    options.addOption(path);
    options.addOption(file);
    options.addOption(command);
    options.addOption(all);
    options.addOption(schema);
    options.addOption(segment);
    options.addOption(tblProperties);
    options.addOption(detail);
    options.addOption(columnName);
    return options;
  }

  public static void main(String[] args) {
    run(args, System.out);
  }

  static void run(String[] args, PrintStream out) {
    Options options = buildOptions();
    CommandLineParser parser = new PosixParser();

    CommandLine line;
    try {
      line = parser.parse(options, args);
    } catch (ParseException exp) {
      out.println("Parsing failed. Reason: " + exp.getMessage());
      return;
    }

    if (line.hasOption("h")) {
      printHelp(options);
      return;
    }

    String path = "";
    if (line.hasOption("p")) {
      path = line.getOptionValue("path");
    }
    out.println("Input Folder: " + path);

    String cmd = line.getOptionValue("cmd");
    Command command;
    if (cmd.equalsIgnoreCase("summary")) {
      command = new DataSummary(path, out);
    } else if (cmd.equalsIgnoreCase("benchmark")) {
      command = new ScanBenchmark(path, out);
    } else {
      out.println("command " + cmd + " is not supported");
      printHelp(options);
      return;
    }

    try {
      command.run(line);
      out.flush();
    } catch (IOException | MemoryException e) {
      e.printStackTrace();
    }
  }

  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("CarbonCli", options);
  }

}
