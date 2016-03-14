/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.filesystem.HDFSMolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.LocalMolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * @author R00900208
 *
 */
public final class FileFactory
{
    private static Configuration configuration = null;
    
    private static FileType storeDefaultFileType  = FileType.LOCAL;
    
    static 
    {
//        boolean shortCircuitRead = Boolean.valueOf(MolapProperties.getInstance().getProperty("dfs.client.read.shortcircuit","true"));
//        boolean shortCircuitUser = Boolean.valueOf(MolapProperties.getInstance().getProperty("dfs.block.local-path-access.user","root"));
        String property = MolapUtil.getCarbonStorePath(null, null)/*MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION)*/;
        if(property != null)
        {
            if(property.startsWith("hdfs://"))
            {
                storeDefaultFileType = FileType.HDFS;
            }
        }
        
        configuration= new Configuration();
        configuration.addResource(new Path("../core-default.xml"));
//        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        if(shortCircuitRead)
//        {
//            configuration.set("dfs.client.read.shortcircuit", "true");
//            configuration.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs-new/dn_socket");
//            configuration.set("dfs.block.local-path-access.user", "hadoop,root");
//        }
//        
//        System.out.println("XXXXXXX FileFactory: dfs.client.read.shortcircuit = " + configuration.get("dfs.client.read.shortcircuit"));
//        System.out.println("XXXXXXX FileFactory: dfs.block.local-path-access.user = " + configuration.get("dfs.block.local-path-access.user"));
//        System.out.println("XXXXXXX FileFactory: dfs.domain.socket.path = " + configuration.get("dfs.block.local-path-access.user"));
    }
    
    private FileFactory()
    {
        
    }
    
    public static Configuration getConfiguration()
    {
        return configuration;
    }
    
    public static FileHolder getFileHolder(FileType fileType)
    {
        switch(fileType)
        {
        case LOCAL : 
            return new FileHolderImpl();
        case HDFS : 
            return new HDFSFileHolderImpl();
        default :
            return new FileHolderImpl();
        }
    }
    
    public static FileType getFileType()
    {
        String property = MolapUtil.getCarbonStorePath(null, null)/*MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION)*/;
        if(property != null)
        {
            if(property.startsWith("hdfs://"))
            {
                storeDefaultFileType = FileType.HDFS;
            }
        }
        return storeDefaultFileType;
    }
    
    public static FileType getFileType(String path)
    {
        if(path.startsWith("hdfs://"))
        {
            return FileType.HDFS;
        }
        return FileType.LOCAL;
    }
    
    public static MolapFile getMolapFile(String path,FileType fileType)
    {
        switch(fileType)
        {
        case LOCAL : 
            return new LocalMolapFile(path);
        case HDFS : 
            return new HDFSMolapFile(path);
        default :
            return new LocalMolapFile(path);
        }
    }
    
    public static DataInputStream getDataInputStream(String path,FileType fileType) throws IOException
    {
        path = path.replace("\\", "/");
        switch(fileType)
        {
        case LOCAL : 
            return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        case HDFS : 
            Path pt=new Path(path);
            FileSystem fs = pt.getFileSystem(configuration);
            FSDataInputStream stream = fs.open(pt);
            return stream;
        default :
            return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        }
    }
    
    public static DataInputStream getDataInputStream(String path,FileType fileType, int bufferSize) throws IOException
    {
        path = path.replace("\\", "/");
        switch(fileType)
        {
        case LOCAL : 
            return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        case HDFS : 
            Path pt=new Path(path);
            FileSystem fs = pt.getFileSystem(configuration);
            FSDataInputStream stream = fs.open(pt,bufferSize);
            return stream;
        default :
            return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        }
    }
//    public static BufferedInputStream getBufferedInputStream(String path, FileType fileType, int bufferSize)  throws IOException
//    {
//        path = path.replace("\\", "/");
//        switch(fileType)
//        {
//        case LOCAL:
//            return new BufferedInputStream(new FileInputStream(path), bufferSize);
//        case HDFS:
//            Path pt = new Path(path);
//            FileSystem fs = FileSystem.get(configuration);
//            BufferedInputStream stream = new BufferedInputStream(fs.open(pt), bufferSize);
//            return stream;
//        default:
//            return new BufferedInputStream(new FileInputStream(path), bufferSize);
//        }
//    }
    
//    public static BufferedInputStream getBufferedInputStream(String path, FileType fileType)  throws IOException
//    {
//        path = path.replace("\\", "/");
//        switch(fileType)
//        {
//        case LOCAL:
//            return new BufferedInputStream(new FileInputStream(path));
//        case HDFS:
//            Path pt = new Path(path);
//            FileSystem fs = FileSystem.get(configuration);
//            BufferedInputStream stream = new BufferedInputStream(fs.open(pt));
//            return stream;
//        default:
//            return new BufferedInputStream(new FileInputStream(path));
//        }
//    }
    
//    public static BufferedOutputStream getBufferedOutputStream(String path, FileType fileType, int bufferSize, boolean append)  throws IOException
//    {
//        path = path.replace("\\", "/");
//        switch(fileType)
//        {
//        case LOCAL:
//            return new BufferedOutputStream(new FileOutputStream(path), bufferSize);
//        case HDFS:
//            Path pt = new Path(path);
//            FileSystem fs = FileSystem.get(configuration);
//            BufferedOutputStream stream = new BufferedOutputStream(fs.create(pt,append), bufferSize);
//            return stream;
//        default:
//            return new BufferedOutputStream(new FileOutputStream(path), bufferSize);
//        }
//    }
//    
//    public static BufferedOutputStream getBufferedOutputStream(String path, FileType fileType, boolean append)  throws IOException
//    {
//        path = path.replace("\\", "/");
//        switch(fileType)
//        {
//        case LOCAL:
//            return new BufferedOutputStream(new FileOutputStream(path));
//        case HDFS:
//            Path pt = new Path(path);
//            FileSystem fs = FileSystem.get(configuration);
//            BufferedOutputStream stream = new BufferedOutputStream(fs.create(pt,append));
//            return stream;
//        default:
//            return new BufferedOutputStream(new FileOutputStream(path));
//        }
//    }
    

    public static DataOutputStream getDataOutputStream(String path,FileType fileType) throws IOException
    {
        path = path.replace("\\", "/");
        switch(fileType)
        {
        case LOCAL : 
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        case HDFS : 
            Path pt=new Path(path);
            FileSystem fs = pt.getFileSystem(configuration);
            FSDataOutputStream stream = fs.create(pt, true);
            return stream;
        default :
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        }
    }
    
    public static DataOutputStream getDataOutputStream(String path,FileType fileType, short replicationFactor) throws IOException
    {
        path = path.replace("\\", "/");
        switch(fileType)
        {
        case LOCAL : 
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        case HDFS : 
            Path pt=new Path(path);
            FileSystem fs = pt.getFileSystem(configuration);
            FSDataOutputStream stream = fs.create(pt, replicationFactor);
            return stream;
        default :
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        }
    }
    
    public static DataOutputStream getDataOutputStream(String path,FileType fileType, int bufferSize) throws IOException
    {
        path = path.replace("\\", "/");
        switch(fileType)
        {
        case LOCAL : 
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), bufferSize));
        case HDFS : 
            Path pt=new Path(path);
            FileSystem fs = pt.getFileSystem(configuration);
            FSDataOutputStream stream = fs.create(pt, true, bufferSize);
            return stream;
        default :
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), bufferSize));
        }
    }
    
    public static DataOutputStream getDataOutputStream(String path,FileType fileType, int bufferSize, boolean append) throws IOException
    {
        path = path.replace("\\", "/");
        switch(fileType)
        {
        case LOCAL : 
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path,append), bufferSize));
        case HDFS : 
            Path pt=new Path(path);
            FileSystem fs = pt.getFileSystem(configuration);
            FSDataOutputStream stream = fs.create(pt, append, bufferSize);
            return stream;
        default :
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), bufferSize));
        }
    }
    
    public static List<String> getFileNames(final String extn, String folderPath, FileType fileType) throws IOException
    {
        folderPath = folderPath.replace("\\", "/");
        List<String> fileNames  = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        switch(fileType)
        {
            case HDFS : 
                Path pt=new Path(folderPath);
                FileSystem fs = pt.getFileSystem(configuration);
                FileStatus[] hdfsFiles =  fs.listStatus(pt);
                for(FileStatus fileStatus : hdfsFiles)
                {
                    if(extn!=null)
                    {
                        if(!fileStatus.isDir() && fileStatus.getPath().getName().endsWith(extn))
                        {
                            fileNames.add(fileStatus.getPath().getName().replace(extn, ""));
                        }
                    }
                    else
                    {
                        if(!fileStatus.isDir())
                        {
                            fileNames.add(fileStatus.getPath().getName());
                        }
                    }
                    
                }
                break;
            case LOCAL : 
            default :
                File[] files = new File(folderPath).listFiles(new FileFilter()
                {
                    @Override
                    public boolean accept(File pathname)
                    {
                        if(pathname.isDirectory())
                            {
                            return false;
                            }
                        
                        if(extn != null)
                        {
                            return pathname.getName().endsWith(extn);
                        }
                        return true;
                    }
                });
                
                for(File oneFile : files)
                {
                    if(extn!=null)
                    {
                        fileNames.add(oneFile.getName().replace(extn, ""));
                    }
                    else
                    {
                        fileNames.add(oneFile.getName());
                    }
                }
        }
        
        return fileNames;
    }
    
    public enum FileType
    {
        LOCAL,HDFS
    }
    
    /**
     * This method checks the given path exists or not and also is it file or
     * not if the performFileCheck is true
     * 
     * @param filePath
     *            - Path
     * @param fileType
     *            - FileType Local/HDFS
     * @param performFileCheck
     *            - Provide false for folders, true for files and
     * 
     */
    public static boolean isFileExist(String filePath,FileType fileType, boolean performFileCheck) throws IOException
    {
        filePath = filePath.replace("\\", "/");
        switch(fileType)
        {
            case HDFS : 
                Path path=new Path(filePath);
                FileSystem fs = path.getFileSystem(configuration);
                if(performFileCheck)
                {
                    return fs.exists(path) && fs.isFile(path);
                }
                else
                {
                    return fs.exists(path);
                }
                
            case LOCAL : 
            default :
                File defaultFile = new File(filePath);
                
                if(performFileCheck)
                {
                    return  defaultFile.exists()  && defaultFile.isFile();
                }
                else
                {
                    return  defaultFile.exists();
                }
        }
    }
    
    /**
     * This method checks the given path exists or not and also is it file or
     * not if the performFileCheck is true
     * 
     * @param filePath
     *            - Path
     * @param fileType
     *            - FileType Local/HDFS
     * @param performFileCheck
     *            - Provide false for folders, true for files and
     * 
     */
    public static boolean isFileExist(String filePath,FileType fileType) throws IOException
    {
        filePath = filePath.replace("\\", "/");
        switch(fileType)
        {
        case HDFS : 
            Path path=new Path(filePath);
            FileSystem fs = path.getFileSystem(configuration);
            return fs.exists(path);
            
        case LOCAL : 
        default :
            File defaultFile = new File(filePath);
                return  defaultFile.exists();
        }
    }
    
    
    public static boolean createNewFile(String filePath,FileType fileType) throws IOException
    {
        filePath = filePath.replace("\\", "/");
        switch(fileType)
        {
        case HDFS : 
            Path path = new Path(filePath);
            FileSystem fs = path.getFileSystem(configuration);
            return fs.createNewFile(path);
            
       case LOCAL : 
        default :
            File file = new File(filePath);
            return file.createNewFile();
        }
    }

    public static boolean mkdirs(String filePath, FileType fileType) throws IOException
    {
        filePath = filePath.replace("\\", "/");
        switch(fileType)
        {
        case HDFS:
            Path path = new Path(filePath);
            FileSystem fs = path.getFileSystem(configuration);
            return fs.mkdirs(path);
        case LOCAL:
        default:
            File file = new File(filePath);
            return file.mkdirs();
        }
    }
    
    /**
     * for getting the dataoutput stream using the hdfs filesystem append API.
     * 
     * @param path
     * @param fileType
     * @return
     * @throws IOException
     */
    public static DataOutputStream getDataOutputStreamUsingAppend(String path,FileType fileType) throws IOException
    {
        path = path.replace("\\", "/");
        switch(fileType)
        {
        case LOCAL : 
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path,true)));
        case HDFS : 
            Path pt=new Path(path);
            FileSystem fs = pt.getFileSystem(configuration);
            FSDataOutputStream stream = fs.append(pt);
            return stream;
        default :
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        }
    }
    
    /**
     * for creating a new Lock file and if it is successfully created
     * then in case of abrupt shutdown then the stream to that file will be closed.
     * @param filePath
     * @param fileType
     * @return
     * @throws IOException
     */
    public static boolean createNewLockFile(String filePath,FileType fileType) throws IOException
    {
        filePath = filePath.replace("\\", "/");
        switch(fileType)
        {
        case HDFS : 
            Path path = new Path(filePath);
            FileSystem fs = path.getFileSystem(configuration);
            if(fs.createNewFile(path))
            {
                fs.deleteOnExit(path);
                return true;
            }
            return false;
       case LOCAL : 
        default :
            File file = new File(filePath);
            return file.createNewFile();
        }
    }
    
}
