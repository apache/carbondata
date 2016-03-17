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

package com.huawei.unibi.molap.engine.mondriantest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class QueryFileReader
{
    /**
     * @param queryFilePath
     * @return
     */
    public static Map<Integer, String> getQueryListFromFile(String queryFilePath)
    {

        Map<Integer, String> a = new HashMap<Integer, String>();
        File file = new File(queryFilePath);
        try
        {

            File[] fileList = file.listFiles();
            int index = 1;
            for(int i = 0;i < fileList.length;i++)
            {

                FileReader reader = new FileReader(fileList[i]);
                BufferedReader br = new BufferedReader(reader);
                String strLine;
                StringBuilder builder = new StringBuilder();
                // Read File Line By Line
                while((strLine = br.readLine()) != null)
                {
                    // Print the content on the console
                    builder.append(strLine);
                    builder.append(" ");
                }
                a.put(index, builder.toString().trim());
                index++;
                // Close the input stream
                br.close();
            }
        }
        catch(Exception e)
        {// Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
        return a;
    }

}
