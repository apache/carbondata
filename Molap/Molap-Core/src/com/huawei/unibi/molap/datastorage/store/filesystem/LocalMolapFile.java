/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.filesystem;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;

/**
 * @author R00900208
 *
 */
public class LocalMolapFile implements MolapFile
{
    private File file;
    
    private static final LogService LOGGER = LogServiceFactory.getLogService(LocalMolapFile.class.getName());

    
    public LocalMolapFile(String path)
    {
        file = new File(path);
    }
    
    public LocalMolapFile(File file)
    {
        this.file = file;
    }

    @Override
    public String getAbsolutePath()
    {
        return file.getAbsolutePath();
    }

    @Override
    public MolapFile[] listFiles(final MolapFileFilter fileFilter)
    {
        if(!file.isDirectory())
        {
            return null;
        }
        
        File[] files = file.listFiles(new FileFilter()
        {
            
            @Override
            public boolean accept(File pathname)
            {
                return fileFilter.accept(new LocalMolapFile(pathname));
            }
        });
        
        if(files == null)
        {
            return new MolapFile[0];
        }
        
        MolapFile[] molapFiles = new MolapFile[files.length];
        
        for(int i = 0;i < molapFiles.length;i++)
        {
            molapFiles[i] = new LocalMolapFile(files[i]);
        }
        
        return molapFiles;
    }

    @Override
    public String getName()
    {
        return file.getName();
    }

    @Override
    public boolean isDirectory()
    {
        return file.isDirectory();
    }

    @Override
    public boolean exists()
    {
        return file.exists();
    }

    @Override
    public String getCanonicalPath()
    {
        try
        {
            return file.getCanonicalPath();
        }
        catch(IOException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,e,"Exception occured"+e.getMessage());
        }
        return null;
    }

    @Override
    public MolapFile getParentFile()
    {
        return new LocalMolapFile(file.getParentFile());
    }

    @Override
    public String getPath()
    {
        return file.getPath();
    }

    @Override
    public long getSize()
    {
        return file.length();
    }

    public boolean renameTo(String changetoName)
    {
        return file.renameTo(new File(changetoName));
    }
    
    public boolean delete()
    {
        return file.delete();
    }

    @Override
    public MolapFile[] listFiles()
    {

        if(!file.isDirectory())
        {
            return null;
        }
        File[] files = file.listFiles();
        if(files == null)
        {
            return new MolapFile[0];
        }
        MolapFile[] molapFiles = new MolapFile[files.length];
        for(int i = 0;i < molapFiles.length;i++)
        {
            molapFiles[i] = new LocalMolapFile(files[i]);
        }
        
        return molapFiles;
    
    }

    @Override
    public boolean createNewFile()
    {
        try
        {
            return file.createNewFile();
        }
        catch(IOException e)
        {
            return false;
        }
    }

    /* (non-Javadoc)
     * @see com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile#mkdirs()
     */
    @Override
    public boolean mkdirs()
    {
        return file.mkdirs();
    }

    @Override
    public long getLastModifiedTime()
    {
        return file.lastModified();
    }

    @Override
    public boolean setLastModifiedTime(long timestamp)
    {
        return file.setLastModified(timestamp);
    }

    @Override
    public boolean renameForce(String changetoName)
    {
        File destFile = new File(changetoName);
        if(destFile.exists())
        {
           if(destFile.delete())
           {
               return file.renameTo(new File(changetoName));
           }
        }
        
        return file.renameTo(new File(changetoName));
        
    }
}
