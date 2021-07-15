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

package org.apache.carbondata.acid;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.acid.transaction.CarbonTransactionManagerFactory;
import org.apache.carbondata.acid.transaction.TableGroupDetail;
import org.apache.carbondata.acid.transaction.TransactionDetail;
import org.apache.carbondata.acid.transaction.TransactionOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

public class TestTransactionManager {

  //TODO: delete this file after the test
  public static void main(String[] args) {

    List<TransactionOperation> operationList = new ArrayList<>();
    operationList.add(TransactionOperation.LOAD);
    operationList.add(TransactionOperation.INSERT);
    operationList.add(TransactionOperation.DELETE);
    ArrayList<AbsoluteTableIdentifier> list = new ArrayList<>(1);
    list.add(AbsoluteTableIdentifier.from("/home/root1/ab/txn"));
    TableGroupDetail tableGroupDetail =
        new TableGroupDetail("table1", "/home/root1/ab/txn", "/home/root1/ab/txn",
            list);

    TransactionDetail detail = CarbonTransactionManagerFactory.getInstance()
        .registerTransaction(false, operationList, tableGroupDetail);

    System.out.println("Transaction id is :" + detail.getTransactionId());

    // avoid updating table ststus file

    CarbonTransactionManagerFactory.getInstance().commitTransaction(detail);

    operationList = new ArrayList<>();
    operationList.add(TransactionOperation.UPDATE);
    TransactionDetail detail1 = CarbonTransactionManagerFactory.getInstance()
        .registerTransaction(false, operationList,
            tableGroupDetail);
    System.out.println("Transaction id is :" + detail1.getTransactionId());

    CarbonTransactionManagerFactory.getInstance().commitTransaction(detail1);

    operationList = new ArrayList<>();
    operationList.add(TransactionOperation.DELETE);
    operationList.add(TransactionOperation.INSERT);
    TransactionDetail detail2 = CarbonTransactionManagerFactory.getInstance()
        .registerTransaction(false, operationList,
            tableGroupDetail);
    System.out.println("Transaction id is :" + detail2.getTransactionId());

    CarbonTransactionManagerFactory.getInstance().commitTransaction(detail2);
  }

}
