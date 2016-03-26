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

public class MolapFolderComparator implements Comparator<File> {

    /**
     * Below method will be used to compare two file
     *
     * @param o1 first file
     * @param o2 Second file
     * @return compare result
     */
    @Override public int compare(File o1, File o2) {
        String firstFileName = o1.getName();
        String secondFileName = o2.getName();
        int lastIndexOffile1 = firstFileName.lastIndexOf('_');
        int lastIndexOffile2 = secondFileName.lastIndexOf('_');
        int f1 = 0;
        int f2 = 0;

        try {
            f1 = Integer.parseInt(firstFileName.substring(lastIndexOffile1 + 1));
            f2 = Integer.parseInt(secondFileName.substring(lastIndexOffile2 + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
        return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
    }
}
