package org.apache.carbondata.core.datastore.impl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.http.client.methods.HttpGet;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.*;
import static org.junit.Assert.assertTrue;

public class CarbonS3FileSystemTest {

    CarbonS3FileSystem carbonS3FileSystem = new CarbonS3FileSystem();
    Path path = new Path("/path");
    File stagingDir;
    int bytesRead = 4;
    boolean first = true;

    @Before
    public void setup() throws IOException {
        stagingDir = new File("stagingDir");
        stagingDir.createNewFile();
    }

    @After
    public void tearDown() {
        stagingDir.delete();
    }

    @Test
    public void testGetFileStatusForNonEmptyPathAndNullMetadata() throws IOException {

        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "/path";
            }

            @Mock
            public Path makeQualified(FileSystem fs) {
                return path;
            }

            @Mock
            public boolean isAbsolute() {
                return true;
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return null;
            }

            @Mock
            public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
                ObjectListing objectListing = new ObjectListing();
                List<String> prefixes = new ArrayList<String>();
                prefixes.add("prefix");
                objectListing.setCommonPrefixes(prefixes);
                return objectListing;
            }
        };

        writeFields();
        FileStatus fileStatus = carbonS3FileSystem.getFileStatus(path);
        assertTrue(fileStatus.getLen() == 0);
        assertTrue(fileStatus.isDirectory());
    }

    // Done
    @Test
    public void testGetFileStatusForEmptyPath() {

        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "";
            }

            @Mock
            public Path makeQualified(FileSystem fs) {
                return path;
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        writeFields();
        try {
            FileStatus fileStatus = carbonS3FileSystem.getFileStatus(path);
            Assert.assertEquals(0, fileStatus.getLen());
            Assert.assertEquals(path, fileStatus.getPath());
        } catch (IOException e) {
            assertTrue(false);
        }
    }

    @Test
    public void testGetFileStatusForEmptyPathAndNullMetadata() {
        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "";
            }

            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path");
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return null;
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };

        writeFields();
        try {
            carbonS3FileSystem.getFileStatus(path);
        } catch (FileNotFoundException fileNotFoundException) {
            assertTrue(true);
        } catch (IOException e) {
            assertTrue(false);
        }
    }

    private void writeFields() {
        try {
            Field s3field = carbonS3FileSystem.getClass().getDeclaredField("s3");
            Field urifield = carbonS3FileSystem.getClass().getDeclaredField("uri");

            s3field.setAccessible(true);
            urifield.setAccessible(true);
            FieldUtils.writeDeclaredField(carbonS3FileSystem, "s3", new AmazonS3Client(), true);
            FieldUtils.writeDeclaredField(carbonS3FileSystem, "uri", new URI("uri"), true);
        } catch (NoSuchFieldException | IllegalAccessException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    //Done
    @Test
    public void testGetFileStatusForIllegalArgumentException() {
        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "";
            }

            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path");
            }
        };

        new MockUp<AmazonS3Client>() {
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return null;
            }
        };

        new MockUp<URI>() {
            public String getHost() {
                return "host";
            }
        };

        writeFields();
        try {
            carbonS3FileSystem.getFileStatus(path);
        } catch (IllegalArgumentException illegalArgumentException) {
            assertTrue(true);
        } catch (IOException e) {
            assertTrue(false);
        }
    }

    @Test
    public void initializeTest() throws URISyntaxException, IOException {
        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(S3_ACCESS_KEY, "access_key");
        carbonProperties.addProperty(S3_SECRET_KEY, "secret_key");
        carbonProperties.addProperty(S3_MAX_CLIENT_RETRIES, "2");
        carbonProperties.addProperty(S3_MAX_ERROR_RETRIES, "1");
        Configuration configuration = new Configuration();
        configuration.set(S3_STAGING_DIRECTORY, "staging_dir");
        configuration.set(S3_MAX_CLIENT_RETRIES, "2");
        configuration.set(S3_MAX_ERROR_RETRIES, "1");

        carbonS3FileSystem.initialize(new URI("uri"), configuration);
        assertTrue(carbonS3FileSystem.getS3Client().getRegionName().equals("us-west-2"));
    }

    @Test
    public void listStatusTest() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Field uri = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uri.setAccessible(true);
        uri.set(carbonS3FileSystem, new URI("uri"));
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, new AmazonS3Client());

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
                ObjectListing objectListing = new ObjectListing();
                List<String> prefixes = new ArrayList<String>();
                prefixes.add("prefix");
                objectListing.setCommonPrefixes(prefixes);
                return objectListing;
            }
        };
        FileStatus[] fileStatuses = carbonS3FileSystem.listStatus(new Path("/path", "dir"));
        assertTrue(fileStatuses[0].getPath().getName().equals("prefix"));
    }

    @Test
    public void openTest() throws IOException, NoSuchFieldException, IllegalAccessException, URISyntaxException {
        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, new AmazonS3Client());

        Field uri = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uri.setAccessible(true);
        uri.set(carbonS3FileSystem, new URI("uri"));
        Field maxAttempts = carbonS3FileSystem.getClass().getDeclaredField("maxAttempts");
        maxAttempts.setAccessible(true);
        maxAttempts.set(carbonS3FileSystem, 2);
        Field maxBackoffTime = carbonS3FileSystem.getClass().getDeclaredField("maxBackoffTime");
        maxBackoffTime.setAccessible(true);
        maxBackoffTime.set(carbonS3FileSystem, Duration.millis(1000));
        Field maxRetryTime = carbonS3FileSystem.getClass().getDeclaredField("maxRetryTime");
        maxRetryTime.setAccessible(true);
        maxRetryTime.set(carbonS3FileSystem, Duration.millis(1000));
        FSDataInputStream fsDataInputStream = carbonS3FileSystem.open(new Path("/path", "dir"), 1024);
        assertTrue(fsDataInputStream.getWrappedStream() instanceof BufferedFSInputStream);
    }

    @Test
    public void createTest() throws IOException, NoSuchFieldException, IllegalAccessException, URISyntaxException {


        Field stagingDirectory = carbonS3FileSystem.getClass().getDeclaredField("stagingDirectory");
        stagingDirectory.setAccessible(true);
        stagingDirectory.set(carbonS3FileSystem, stagingDir);
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, new AmazonS3Client());
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        Field sseType = carbonS3FileSystem.getClass().getDeclaredField("sseType");
        sseType.setAccessible(true);
        sseType.set(carbonS3FileSystem, CarbonS3FileSystem.CarbonS3SseType.S3);
        new MockUp<Files>() {
            @Mock
            public java.nio.file.Path createDirectories(java.nio.file.Path var0, FileAttribute... var1) {
                try {
                    return ((File) stagingDirectory.get(carbonS3FileSystem)).toPath();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                return var0;
            }

            @Mock
            public java.nio.file.Path createTempFile(java.nio.file.Path var0, String var1, String var2, FileAttribute... var3) throws IllegalAccessException {
                return ((File) stagingDirectory.get(carbonS3FileSystem)).toPath();
            }
        };

        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }
        };

        new MockUp<Path>() {
            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path", "dir");
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };
        FSDataOutputStream fsDataOutputStream = carbonS3FileSystem.create(new Path("/path", "dir"), new FsPermission(FsAction.ALL, FsAction.READ_WRITE, FsAction.READ), true, 1024, new Short("2"), 128L, new TaskAttemptContextImpl(new JobConf(), new TaskAttemptID()));

        assertTrue(fsDataOutputStream.getWrappedStream() instanceof CarbonS3FileSystem.CarbonS3OutputStream);
    }

    @Test(expected = IOException.class)
    public void createTestExceptionCase() throws IOException, NoSuchFieldException, IllegalAccessException, URISyntaxException {


        Field stagingDirectory = carbonS3FileSystem.getClass().getDeclaredField("stagingDirectory");
        stagingDirectory.setAccessible(true);
        stagingDirectory.set(carbonS3FileSystem, stagingDir);
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, new AmazonS3Client());
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        Field sseType = carbonS3FileSystem.getClass().getDeclaredField("sseType");
        sseType.setAccessible(true);
        sseType.set(carbonS3FileSystem, CarbonS3FileSystem.CarbonS3SseType.S3);
        new MockUp<Files>() {
            @Mock
            public java.nio.file.Path createDirectories(java.nio.file.Path var0, FileAttribute... var1) {
                try {
                    return ((File) stagingDirectory.get(carbonS3FileSystem)).toPath();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                return var0;
            }

            @Mock
            public java.nio.file.Path createTempFile(java.nio.file.Path var0, String var1, String var2, FileAttribute... var3) throws IllegalAccessException {
                return ((File) stagingDirectory.get(carbonS3FileSystem)).toPath();
            }
        };

        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return false;
            }
        };

        new MockUp<Path>() {
            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path", "dir");
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };
        carbonS3FileSystem.create(new Path("/path", "dir"), new FsPermission(FsAction.ALL, FsAction.READ_WRITE, FsAction.READ), true, 1024, new Short("2"), 128L, new TaskAttemptContextImpl(new JobConf(), new TaskAttemptID()));
    }

    @Test
    public void appendTest() throws NoSuchFieldException, IllegalAccessException, URISyntaxException {
        Field stagingDirectory = carbonS3FileSystem.getClass().getDeclaredField("stagingDirectory");
        stagingDirectory.setAccessible(true);
        stagingDirectory.set(carbonS3FileSystem, stagingDir);
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        Field sseType = carbonS3FileSystem.getClass().getDeclaredField("sseType");
        sseType.setAccessible(true);
        sseType.set(carbonS3FileSystem, CarbonS3FileSystem.CarbonS3SseType.S3);
        new MockUp<Path>() {
            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path");
            }
        };
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }
        };
        new MockUp<Files>() {
            @Mock
            public java.nio.file.Path createTempFile(java.nio.file.Path var0, String var1, String var2, FileAttribute... var3) throws IllegalAccessException {
                return ((File) stagingDirectory.get(carbonS3FileSystem)).toPath();
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }

            @Mock
            public S3Object getObject(GetObjectRequest getObjectRequest) {
                return new S3Object();
            }
        };

        new MockUp<S3Object>() {
            @Mock
            public S3ObjectInputStream getObjectContent() {
                return new S3ObjectInputStream(new CarbonS3FileSystem.CarbonS3InputStream(amazonS3Client, "host", new Path("/path"), 2, Duration.millis(1000), Duration.millis(1000)), new HttpGet());
            }
        };

        new MockUp<CarbonS3FileSystem.CarbonS3InputStream>() {
            @Mock
            public int read(byte[] buffer, int offset, int length) {
                return bytesRead--;
            }
        };

        FSDataOutputStream fsDataOutputStream = carbonS3FileSystem.append(new Path("/path", "dir"), 1024, new TaskAttemptContextImpl(new JobConf(), new
                TaskAttemptID()));
        assertTrue(fsDataOutputStream.getWrappedStream() instanceof CarbonS3FileSystem.CarbonS3OutputStream);
    }

    @Test
    public void appendTestFileNotExist() throws NoSuchFieldException, IllegalAccessException, URISyntaxException {
        Field stagingDirectory = carbonS3FileSystem.getClass().getDeclaredField("stagingDirectory");
        stagingDirectory.setAccessible(true);
        stagingDirectory.set(carbonS3FileSystem, stagingDir);
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        Field sseType = carbonS3FileSystem.getClass().getDeclaredField("sseType");
        sseType.setAccessible(true);
        sseType.set(carbonS3FileSystem, CarbonS3FileSystem.CarbonS3SseType.S3);
        new MockUp<Path>() {
            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path");
            }
        };
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }
        };
        new MockUp<Files>() {
            @Mock
            public java.nio.file.Path createTempFile(java.nio.file.Path var0, String var1, String var2, FileAttribute... var3) throws IllegalAccessException {
                return ((File) stagingDirectory.get(carbonS3FileSystem)).toPath();
            }
        };

        new MockUp<FileSystem>() {
            @Mock
            public boolean exists(Path f) throws IOException {
                return false;
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };
        FSDataOutputStream fsDataOutputStream = carbonS3FileSystem.append(new Path("/path", "dir"), 1024, new TaskAttemptContextImpl(new JobConf(), new
                TaskAttemptID()));
        assertTrue(fsDataOutputStream.getWrappedStream() instanceof CarbonS3FileSystem.CarbonS3OutputStream);
    }

    @Test(expected = RuntimeException.class)
    public void appendTestExceptionCase() throws NoSuchFieldException, IllegalAccessException, URISyntaxException {
        Field stagingDirectory = carbonS3FileSystem.getClass().getDeclaredField("stagingDirectory");
        stagingDirectory.setAccessible(true);
        stagingDirectory.set(carbonS3FileSystem, stagingDir);
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        Field sseType = carbonS3FileSystem.getClass().getDeclaredField("sseType");
        sseType.setAccessible(true);
        sseType.set(carbonS3FileSystem, CarbonS3FileSystem.CarbonS3SseType.S3);
        new MockUp<Path>() {
            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path");
            }
        };
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }
        };
        new MockUp<Files>() {
            @Mock
            public java.nio.file.Path createTempFile(java.nio.file.Path var0, String var1, String var2, FileAttribute... var3) throws IllegalAccessException {
                return ((File) stagingDirectory.get(carbonS3FileSystem)).toPath();
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }

            @Mock
            public S3Object getObject(GetObjectRequest getObjectRequest) {
                return new S3Object();
            }
        };

        new MockUp<S3Object>() {
            @Mock
            public S3ObjectInputStream getObjectContent() {
                return new S3ObjectInputStream(new CarbonS3FileSystem.CarbonS3InputStream(amazonS3Client, "host", new Path("/path"), 2, Duration.millis(1000), Duration.millis(1000)), new HttpGet());
            }
        };

        new MockUp<CarbonS3FileSystem.CarbonS3InputStream>() {
            @Mock
            public int read(byte[] buffer, int offset, int length) throws IOException {
                throw new IOException("Cannot read file");
            }
        };

        carbonS3FileSystem.append(new Path("/path", "dir"), 1024, new TaskAttemptContextImpl(new JobConf(), new
                TaskAttemptID()));
    }

    @Test
    public void renameTest() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Path srcPath = new Path("/src");
        Path destPath = new Path("/dest");
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public void deleteObject(String bucketName, String key) {

            }

            @Mock
            public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
                                               String destinationBucketName, String destinationKey) {
                return new CopyObjectResult();
            }
        };
        assertTrue(carbonS3FileSystem.rename(srcPath, destPath));
    }

    @Test
    public void renameTestWhenSrcIsDir() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Path srcPath = new Path("/src");
        Path destPath = new Path("/dest");
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public void deleteObject(String bucketName, String key) {

            }

            @Mock
            public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
                                               String destinationBucketName, String destinationKey) {
                return new CopyObjectResult();
            }
        };
        new MockUp<FileStatus>() {
            @Mock
            public boolean isDirectory() {
                if (first) {
                    first = false;
                    return true;
                }
                first = true;
                return false;
            }
        };
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
                ObjectListing objectListing = new ObjectListing();
                List<String> prefixes = new ArrayList<String>();
                prefixes.add("prefix");
                objectListing.setCommonPrefixes(prefixes);
                return objectListing;
            }
        };
        assertTrue(carbonS3FileSystem.rename(srcPath, destPath));
    }

    @Test
    public void renameTestExceptionCase() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Path srcPath = new Path("/src");
        Path destPath = new Path("/dest");
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public void deleteObject(String bucketName, String key) {

            }

            @Mock
            public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
                                               String destinationBucketName, String destinationKey) {
                return new CopyObjectResult();
            }
        };
        new MockUp<FileStatus>() {
            @Mock
            public boolean isDirectory() throws FileNotFoundException {
                throw new FileNotFoundException("File not Found");
            }
        };
        assertFalse(carbonS3FileSystem.rename(srcPath, destPath));
    }

    @Test
    public void deleteTest() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public void deleteObject(String bucketName, String key) {

            }
        };
        assertTrue(carbonS3FileSystem.delete(new Path("/path"), false));
    }

    @Test
    public void deleteTestExceptionCase() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public void deleteObject(String bucketName, String key) throws FileNotFoundException {
                throw new FileNotFoundException("File not Found");
            }
        };

        assertFalse(carbonS3FileSystem.delete(new Path("/path"), false));
    }

    @Test(expected = IOException.class)
    public void deleteRecursiveTest() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public void deleteObject(String bucketName, String key) {

            }
        };
        new MockUp<FileStatus>() {
            @Mock
            public boolean isDirectory() throws FileNotFoundException {
                return true;
            }
        };
        carbonS3FileSystem.delete(new Path("/path"), false);
    }

    @Test
    public void deleteTestWhenPathIsNotDir() throws IOException, NoSuchFieldException, URISyntaxException, IllegalAccessException {
        Field uriField = carbonS3FileSystem.getClass().getDeclaredField("uri");
        uriField.setAccessible(true);
        uriField.set(carbonS3FileSystem, new URI("uri"));
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        Field s3 = carbonS3FileSystem.getClass().getDeclaredField("s3");
        s3.setAccessible(true);
        s3.set(carbonS3FileSystem, amazonS3Client);
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public void deleteObject(String bucketName, String key) {

            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
                ObjectListing objectListing = new ObjectListing();
                return objectListing;
            }
        };
        new MockUp<FileStatus>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }
        };
        assertTrue(carbonS3FileSystem.delete(new Path("/path"), true));
    }
}
