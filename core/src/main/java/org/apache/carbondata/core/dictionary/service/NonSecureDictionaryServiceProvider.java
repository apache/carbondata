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

package org.apache.carbondata.core.dictionary.service;

import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.client.NonSecureDictionaryClient;

public class NonSecureDictionaryServiceProvider implements DictionaryServiceProvider {
  private int port = 0;

  /**
   * default serial version ID.
   */
  private static final long serialVersionUID = 1L;

  public NonSecureDictionaryServiceProvider(int port) {
    this.port = port;
  }

  //  @Override public DictionaryServer getDictionaryServer() {
  //    return NonSecureDictionaryServer.getInstance(port);
  //  }

  @Override public DictionaryClient getDictionaryClient() {
    return new NonSecureDictionaryClient();
  }
}
