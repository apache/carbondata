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
