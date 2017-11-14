package org.apache.carbondata.core.locks;


import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.S3CarbonFile;
import org.apache.carbondata.core.datastore.impl.CarbonS3FileSystem;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class S3FileLockTest {
    private File f = new File("lockFile");

    @After public void tearDown() {
        f.delete();
    }

    @Test
    public void lockTest() {

        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewLockFile(String filePath, FileFactory.FileType fileType) {
                return true;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                return false;
            }
            @Mock public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType) throws IOException {
                return new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID"));
            }
        };

        new MockUp<Configured>() {
            @Mock public Configuration getConf() {
                return new Configuration();
            }
        };


        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) {

            }

            @Mock public FileStatus getFileStatus(Path path) throws IOException {
                return new FileStatus(128L, false, 0, 128L, 0L, new Path("lockFile"));
            }
        };

        new MockUp<FSDataOutputStream>() {
            @Mock public void close() {
                return;
            }
        };


        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, "s3a://tmp");
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", "tableId");
        S3FileLock s3FileLock = new S3FileLock(carbonTableIdentifier, "lockFile");
        assertTrue(s3FileLock.lock());

    }

    @Test
    public void lockTestFailureCase() {

        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewLockFile(String filePath, FileFactory.FileType fileType) throws IOException {
                throw new IOException("File cannot be created");
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                return false;
            }
            @Mock public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType) throws IOException {
                return new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID"));
            }
        };

        new MockUp<Configured>() {
            @Mock public Configuration getConf() {
                return new Configuration();
            }
        };


        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) {

            }

        };
        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, "s3a://tmp");
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", "tableId");
        S3FileLock s3FileLock = new S3FileLock(carbonTableIdentifier, "lockFile");
        assertFalse(s3FileLock.lock());

    }

    @Test
    public void unlockTest() throws NoSuchFieldException, IOException, IllegalAccessException {

        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewLockFile(String filePath, FileFactory.FileType fileType) {
                return true;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                return false;
            }
            @Mock public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType) throws IOException {
                return new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID"));
            }
        };

        new MockUp<Configured>() {
            @Mock public Configuration getConf() {
                return new Configuration();
            }
        };


        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) {

            }

            @Mock public FileStatus getFileStatus(Path path) throws IOException {
                return new FileStatus(128L, false, 0, 128L, 0L, new Path("lockFile"));
            }
        };

        new MockUp<FSDataOutputStream>() {
            @Mock public void close() {
                return;
            }
        };

        new MockUp<S3CarbonFile>() {
            @Mock public boolean delete() {
                return false;
            }
        };

        new MockUp<S3CarbonFile>() {
            @Mock public boolean delete() {
                return true;
            }
        };

        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, "s3a://tmp");
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", "tableId");
        S3FileLock s3FileLock = new S3FileLock(carbonTableIdentifier, "lockFile");


        Field dataOutputStream = s3FileLock.getClass().getDeclaredField("dataOutputStream");
        dataOutputStream.setAccessible(true);
        dataOutputStream.set(s3FileLock, new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID")));

        assertTrue(s3FileLock.unlock());
    }

    @Test
    public void unlockTestFailureCase() throws NoSuchFieldException, IOException, IllegalAccessException {

        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewLockFile(String filePath, FileFactory.FileType fileType) {
                return true;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                return false;
            }
            @Mock public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType) throws IOException {
                return new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID"));
            }
        };

        new MockUp<Configured>() {
            @Mock public Configuration getConf() {
                return new Configuration();
            }
        };


        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) {

            }

            @Mock public FileStatus getFileStatus(Path path) throws IOException {
                return new FileStatus(128L, false, 0, 128L, 0L, new Path("lockFile"));
            }
        };

        new MockUp<FSDataOutputStream>() {
            @Mock public void close() {
                return;
            }
        };

        new MockUp<S3CarbonFile>() {
            @Mock public boolean delete() {
                return false;
            }
        };

        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, "s3a://tmp");
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", "tableId");
        S3FileLock s3FileLock = new S3FileLock(carbonTableIdentifier, "lockFile");


        Field dataOutputStream = s3FileLock.getClass().getDeclaredField("dataOutputStream");
        dataOutputStream.setAccessible(true);
        dataOutputStream.set(s3FileLock, new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID")));

        assertFalse(s3FileLock.unlock());
    }

    @Test
    public void unlockFailureDataOutputStreamNotClosed() throws NoSuchFieldException, IOException, IllegalAccessException {

        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewLockFile(String filePath, FileFactory.FileType fileType) {
                return true;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                return false;
            }
            @Mock public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType) throws IOException {
                return new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID"));
            }
        };

        new MockUp<Configured>() {
            @Mock public Configuration getConf() {
                return new Configuration();
            }
        };


        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) {

            }

            @Mock public FileStatus getFileStatus(Path path) throws IOException {
                return new FileStatus(128L, false, 0, 128L, 0L, new Path("lockFile"));
            }
        };

        new MockUp<FSDataOutputStream>() {
            @Mock public void close() throws IOException {
                throw new IOException("Output Stream not Closed");
            }
        };

        new MockUp<S3CarbonFile>() {
            @Mock public boolean delete() {
                return false;
            }
        };

        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, "s3a://tmp");
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", "tableId");
        S3FileLock s3FileLock = new S3FileLock(carbonTableIdentifier, "lockFile");
        Field dataOutputStream = s3FileLock.getClass().getDeclaredField("dataOutputStream");
        dataOutputStream.setAccessible(true);
        dataOutputStream.set(s3FileLock, new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID")));
        assertFalse(s3FileLock.unlock());
    }

    @Test
    public void unlockTestFailureCaseLockFileNotFound() throws NoSuchFieldException, IOException, IllegalAccessException {

        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewLockFile(String filePath, FileFactory.FileType fileType) {
                return true;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                return false;
            }
            @Mock public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType) throws IOException {
                return new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID"));
            }
        };

        new MockUp<Configured>() {
            @Mock public Configuration getConf() {
                return new Configuration();
            }
        };


        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) {
            }

            @Mock public FileStatus getFileStatus(Path path) throws IOException {
                return new FileStatus(128L, false, 0, 128L, 0L, new Path("lockFile"));
            }
        };

        new MockUp<FSDataOutputStream>() {
            @Mock public void close() {
            }
        };

        new MockUp<S3CarbonFile>() {
            @Mock public boolean delete() {
                return false;
            }

            @Mock public boolean exists() {
                return false;
            }
        };

        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, "s3a://tmp");
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", "tableId");
        S3FileLock s3FileLock = new S3FileLock(carbonTableIdentifier, "lockFile");
        Field dataOutputStream = s3FileLock.getClass().getDeclaredField("dataOutputStream");
        dataOutputStream.setAccessible(true);
        dataOutputStream.set(s3FileLock, new FSDataOutputStream(new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, false, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID")));
        assertFalse(s3FileLock.unlock());
    }

}
