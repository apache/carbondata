/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.filesystem;


/**
 * @author R00900208
 *
 */
public interface MolapFileFilter
{
     boolean accept(MolapFile file);
}
