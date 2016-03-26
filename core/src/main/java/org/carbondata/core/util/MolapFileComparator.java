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

package org.carbondata.core.util;

import java.io.File;
import java.util.Comparator;

public class MolapFileComparator implements Comparator<File> {

    /**
     * File extension
     */
    private String fileExt;

    public MolapFileComparator(String fileExt) {
        this.fileExt = fileExt;
    }

    @Override public int compare(File file1, File file2) {
        String firstFileName = file1.getName().split(fileExt)[0];
        String secondFileName = file2.getName().split(fileExt)[0];
        int lastIndexOfO1 = firstFileName.lastIndexOf('_');
        int lastIndexOfO2 = secondFileName.lastIndexOf('_');
        int f1 = 0;
        int f2 = 0;

        try {
            f1 = Integer.parseInt(firstFileName.substring(lastIndexOfO1 + 1));
            f2 = Integer.parseInt(secondFileName.substring(lastIndexOfO2 + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
        return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
    }
}
