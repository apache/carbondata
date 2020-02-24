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

package org.apache.spark.sql;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType;

public class ColumnVectorFactory {

    public static WritableColumnVector[] getColumnVector(MemoryMode memMode, StructType outputSchema, int rowNums) {

        WritableColumnVector[] writableColumnVectors = null;
        switch (memMode) {
            case ON_HEAP:
                writableColumnVectors = OnHeapColumnVector
                        .allocateColumns(rowNums, outputSchema);
                break;
            case OFF_HEAP:
                writableColumnVectors = OffHeapColumnVector
                        .allocateColumns(rowNums, outputSchema);
                break;
        }
        return writableColumnVectors;
    }
}
