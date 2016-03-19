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

package com.huawei.unibi.molap.queryexecutor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.result.MolapResultStreamHolder;

/**
 * It is the remote interface of Spring for executor interface
 */
public interface RemoteMolapQueryInternalExecutor extends Remote
{
	/**
	 * Execute the query
	 * @param molapQuery
	 * @return
	 * @throws RemoteException
	 */
    MolapResultStreamHolder execute(MolapQuery molapQuery) throws RemoteException;

    /**
     * Get the next chunk
     * @param uuid
     * @return
     * @throws RemoteException
     */
    MolapResultStreamHolder getNext(UUID uuid) throws RemoteException;

}
