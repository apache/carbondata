/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.queryexecutor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import com.huawei.unibi.molap.query.MolapQuery;
import com.huawei.unibi.molap.query.result.MolapResultStreamHolder;

/**
 * It is the remote interface of Spring for executor interface
 * @author R00900208
 *
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
