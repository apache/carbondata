package org.apache.carbondata.core.datastore.impl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.HttpGet;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class CarbonS3InputStreamTest {
    AmazonS3Client amazonS3Client = new AmazonS3Client();

    @Test
    public void seekTest() {
        CarbonS3FileSystem.CarbonS3InputStream carbonS3InputStream = new CarbonS3FileSystem.CarbonS3InputStream(amazonS3Client, "host", new Path("/path"), 2, Duration.millis(1000), Duration.millis(1000));
        carbonS3InputStream.seek(2L);
    }

    @Test
    public void readTest() throws IOException {
        CarbonS3FileSystem.CarbonS3InputStream carbonS3InputStream = new CarbonS3FileSystem.CarbonS3InputStream(amazonS3Client, "host", new Path("/path"), 2, Duration.millis(1000), Duration.millis(1000));
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

        new MockUp<S3ObjectInputStream>() {
            @Mock
            public int read(byte[] var1, int var2, int var3) throws IOException {
                return 2;
            }
        };
        assertTrue(carbonS3InputStream.read(new byte[]{1, 2}, 0, 128) == 2);
    }

    @Test(expected = Exception.class)
    public void readTestExceptionCase() throws IOException {
        CarbonS3FileSystem.CarbonS3InputStream carbonS3InputStream = new CarbonS3FileSystem.CarbonS3InputStream(amazonS3Client, "host", new Path("/path"), 2, Duration.millis(1000), Duration.millis(1000));
        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }

            @Mock
            public S3Object getObject(GetObjectRequest getObjectRequest) {
                throw new AmazonS3Exception("Amazon S3 Exception....");
            }
        };
        new MockUp<S3Object>() {
            @Mock
            public S3ObjectInputStream getObjectContent() {
                return new S3ObjectInputStream(new CarbonS3FileSystem.CarbonS3InputStream(amazonS3Client, "host", new Path("/path"), 2, Duration.millis(1000), Duration.millis(1000)), new HttpGet());
            }
        };

        new MockUp<S3ObjectInputStream>() {
            @Mock
            public int read(byte[] var1, int var2, int var3) throws IOException {
                return 2;
            }
        };
        carbonS3InputStream.read(new byte[]{1, 2}, 0, 128);
    }
}
