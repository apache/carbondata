/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.filesystem;

/**
 * @author R00900208
 *
 */
public interface MolapFile
{
    String getAbsolutePath();

    MolapFile[] listFiles(MolapFileFilter fileFilter);

    MolapFile[] listFiles();

    String getName();

    boolean isDirectory();

    boolean exists();

    String getCanonicalPath();

    MolapFile getParentFile();

    String getPath();

    long getSize();

    boolean renameTo(String changetoName);
    
    boolean renameForce(String changetoName);

    boolean delete();

    boolean createNewFile();

    boolean mkdirs();
    
    long getLastModifiedTime();
    
    boolean setLastModifiedTime(long timestamp);
}
