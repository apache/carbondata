package org.apache.carbondata.core.datastore.impl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

import static junit.framework.TestCase.assertTrue;

public class S3FileHolderImplTest {
    private static String filePath;
    private static S3FileHolderImpl s3FileHolder;
    private static CarbonProperties defaults;
    private static byte[] data;
    private static AmazonS3Client amazonS3Client;

    @BeforeClass
    public static void setUp() {
        filePath = "/fakePath";
        s3FileHolder = new S3FileHolderImpl();
        data = "fake data".getBytes();
        amazonS3Client = new AmazonS3Client();
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };
    }

    @Test()
    public void testReadByteBuffer() throws IOException, NoSuchFieldException, IllegalAccessException {
        CarbonS3FileSystem.CarbonS3InputStream carbonS3InputStream = new CarbonS3FileSystem.CarbonS3InputStream(
                amazonS3Client, "host",
                new Path("/path"), 2, Duration.millis(1000), Duration.millis(1000));
        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) throws IOException {

            }
        };
        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public FSDataInputStream open(Path f) throws IOException {

                return new FSDataInputStream(new BufferedFSInputStream(carbonS3InputStream, 20));
            }
        };
        new MockUp<CarbonS3FileSystem.CarbonS3InputStream>() {
            @Mock
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return 20;
            }
        };
        ByteBuffer byteBuffer = s3FileHolder.readByteBuffer(filePath, 50l, 20);
        assertTrue(byteBuffer instanceof ByteBuffer);
    }
}
