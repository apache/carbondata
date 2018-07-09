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

package org.apache.carbondata.sdk.file;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

/**
 * Writer to write row data to carbondata file. Call {@link #builder()} to get
 * a build to create instance of writer.
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public abstract class CarbonWriter {

  /**
   * Write an object to the file, the format of the object depends on the
   * implementation.
   * Note: This API is not thread safe
   */
  public abstract void write(Object object) throws IOException;

  /**
   * Flush and close the writer
   */
  public abstract void close() throws IOException;

  /**
   * Create a {@link CarbonWriterBuilder} to build a {@link CarbonWriter}
   */
  public static CarbonWriterBuilder builder() {
    return new CarbonWriterBuilder();
  }

}
