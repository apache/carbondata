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

package org.apache.carbon.flink;

import org.apache.flink.core.fs.RecoverableWriter;

public final class ProxyRecoverable
        implements RecoverableWriter.CommitRecoverable, RecoverableWriter.ResumeRecoverable {

  public ProxyRecoverable(
      final String writerType,
      final ProxyFileWriterFactory.Configuration writerConfiguration,
      final String writerIdentifier,
      final String writePath
  ) {
    this.writerType = writerType;
    this.writerConfiguration = writerConfiguration;
    this.writerIdentifier = writerIdentifier;
    this.writePath = writePath;
  }

  private final String writerType;

  private final ProxyFileWriterFactory.Configuration writerConfiguration;

  private final String writerIdentifier;

  private final String writePath;

  public String getWriterType() {
    return this.writerType;
  }

  public ProxyFileWriterFactory.Configuration getWriterConfiguration() {
    return this.writerConfiguration;
  }

  public String getWriterIdentifier() {
    return this.writerIdentifier;
  }

  public String getWritePath() {
    return this.writePath;
  }

}
