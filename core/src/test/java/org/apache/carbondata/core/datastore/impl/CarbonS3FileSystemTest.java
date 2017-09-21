package org.apache.carbondata.core.datastore.impl;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.*;

public class CarbonS3FileSystemTest {

    CarbonS3FileSystem carbonS3FileSystem = new CarbonS3FileSystem();

    @Test
    public void testInitialize() {
        new MockUp<URI>() {
            @Mock
            public String getScheme() {
                return "s3a";
            }

            @Mock
            public String getAuthority() {
                return "bucket-name";
            }

            @Mock
            public URI create(String str) throws URISyntaxException {
                return new URI("s3a://bucket-name/tablestatus");
            }
        };

        new MockUp<S3AFileSystem>() {
            @Mock
            public void initialize(URI name, Configuration conf) throws IOException {
            }
        };

        try {
            CarbonProperties.getInstance().addProperty(S3_ACCESS_KEY, "access-key")
                    .addProperty(S3_SECRET_KEY, "secret-key")
                    .addProperty(S3_IMPLEMENTATION, "org.apache.carbondata.core.datastore.impl.CarbonS3FileSystem");
            URI uri = new URI("/uri");
            Configuration conf = new Configuration();
            try {
                carbonS3FileSystem.initialize(uri, conf);
                Assert.assertTrue(carbonS3FileSystem.getUri().toString() == "s3a://bucket-name/tablestatus");
            } catch (IOException e) {
                Assert.assertTrue(false);
            }
        } catch (URISyntaxException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testMkdirs() {
        try {
            Assert.assertTrue(carbonS3FileSystem.mkdirs(new Path("/path"), new FsPermission("777")));
        } catch (IOException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testGetUri() {
        Assert.assertTrue(carbonS3FileSystem.getUri() == null);
    }
}
