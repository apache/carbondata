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

package org.apache.carbondata.acid.transaction;

import org.apache.carbondata.core.util.CarbonProperties;

public final class CarbonTransactionManagerFactory {

  // class name for db based or other transaction manager interface
  private static String transactionManagerImplClass =
      CarbonProperties.getInstance().getProperty("carbon.transaction.manager.interface", null);

  private static TransactionManager INSTANCE;

  private CarbonTransactionManagerFactory() {
  }

  static {
    //TODO: Need to add synchronization ? (may be not required as it is not a lazy initialization)
    if (transactionManagerImplClass == null) {
      // use the default implementation
      INSTANCE = new CarbonFileBasedTransactionManager();
    } else {
      //TODO: later get from the className and use that implementation
      INSTANCE = null;
    }
  }

  public static TransactionManager getInstance() {
    return INSTANCE;
  }

}
