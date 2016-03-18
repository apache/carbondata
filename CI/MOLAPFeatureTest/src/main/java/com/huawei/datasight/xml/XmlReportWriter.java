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

package com.huawei.datasight.xml;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Date;
import java.text.SimpleDateFormat;
import org.apache.commons.lang.StringEscapeUtils;

public class XmlReportWriter
{
  PrintWriter xmlWriter;
  String myclassNAme="FTResult";
  int pass = 0; int fail = 0;
  int count=0;
  StringBuffer buffer = new StringBuffer();

  public XmlReportWriter()
  {
    try {
      String xmlFolderPath = ".";
      File path = new File(xmlFolderPath);
      if (!path.exists()) {
        path.mkdirs();
      }

     

      String myfile = xmlFolderPath + "/TEST-" + this.myclassNAme + ".xml";

      File xmlReport = new File(myfile);

      if (xmlReport.exists()) {
        xmlReport.delete();
      }
      else {
        xmlReport.createNewFile();
      }

      this.xmlWriter = new PrintWriter(xmlReport);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void writePASS(String tc_id, String message)
  {
    this.pass += 1;
    this.buffer.append("<testcase classname=\"").append(this.myclassNAme).append("\"").append(" name=\"").append(count++).append("[").append(StringEscapeUtils.escapeHtml(message)).append("]\"").append(" time=\"0\" />");
    this.buffer.append("\n");
  }

  /*public void writeFAIL(String tc_id, String query)
  {
    this.fail += 1;
    this.buffer.append("<testcase classname=\"").append(this.myclassNAme).append("\"").append(" name=\"").append(count++).append("[").append(StringEscapeUtils.escapeHtml(query)).append("]\"").append(" time=\"0\" >");
    this.buffer.append("\n");
    String datamismatchmesage = "Failed" "Data Mismatched . Please check in the path Expected PATH= " + this.confReader.getEXPECTED_PATH() + "/" + tc_id + ".csv  Actual Path " + this.confReader.getACTUAL_PATH() + "/" + tc_id + ".csv";
    this.buffer.append(" <failure message=\"").append(datamismatchmesage).append("\"");
    this.buffer.append(" type=\"junit.framework.AssertionFailedError\">");
    this.buffer.append(" junit.framework.AssertionFailedError:");
    this.buffer.append(" type=\"junit.framework.AssertionFailedError\">junit.framework.AssertionFailedError: ");
    this.buffer.append(datamismatchmesage);
    this.buffer.append(" </failure>");
    this.buffer.append(" </testcase>");
    this.buffer.append("\n");
  }*/

  public void writeFAIL(String tc_id, String query, String mymessage) {
    this.fail += 1;
    this.buffer.append("<testcase classname=\"").append(this.myclassNAme).append("\"").append(" name=\"").append(count++).append("[").append(StringEscapeUtils.escapeHtml(query)).append("]\"").append(" time=\"0\" >");
    this.buffer.append("\n");
    this.buffer.append(" <failure message=\"").append(StringEscapeUtils.escapeHtml(mymessage)).append("\"");
    this.buffer.append(" type=\"junit.framework.AssertionFailedError\">");
    this.buffer.append(" junit.framework.AssertionFailedError:");
    this.buffer.append(" type=\"junit.framework.AssertionFailedError\">junit.framework.AssertionFailedError: ");
    this.buffer.append(StringEscapeUtils.escapeHtml(mymessage));
    this.buffer.append(" </failure>");
    this.buffer.append(" </testcase>");
    this.buffer.append("\n");
  }

  public void flushToResultFile()
  {
    this.buffer.append("</testsuite>");
    StringBuffer headerString = new StringBuffer();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
    Date date = new Date(System.currentTimeMillis());

    headerString.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
    headerString.append("\n");
    headerString.append(" <testsuite errors=\"0\" ").append(" failures=\"").append(this.fail + "\"").append(" hostname=\"10.19.92.183\" ").append(" name=\"").append(this.myclassNAme).append("\"");
    headerString.append(" tests=\"").append(this.count+ "\" time=\"0\"  timestamp=\"").append(dateFormat.format(date)).append("\">");

    headerString.append("\n");
    headerString.append(this.buffer);
    this.xmlWriter.append(headerString);
    this.xmlWriter.flush();
    this.xmlWriter.close();
  }
}