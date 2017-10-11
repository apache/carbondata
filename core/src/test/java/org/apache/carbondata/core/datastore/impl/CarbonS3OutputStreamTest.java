package org.apache.carbondata.core.datastore.impl;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressListenerChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.AbstractTransfer;
import com.amazonaws.services.s3.transfer.internal.UploadImpl;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;

import static org.junit.Assert.assertTrue;

public class CarbonS3OutputStreamTest {
    private File f = new File("lockFile");

    @After
    public void tearDown() {
        f.delete();
    }

    @Test
    public void closeTest() throws IOException, NoSuchFieldException, IllegalAccessException {
        CarbonS3FileSystem.CarbonS3OutputStream carbonS3OutputStream = new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, true, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID");
        new MockUp<TransferManager>() {
            @Mock
            public Upload upload(final PutObjectRequest putObjectRequest)
                    throws AmazonServiceException, AmazonClientException {
                return new UploadImpl("upload object", new TransferProgress(), new ProgressListenerChain(), null);
            }
        };

        new MockUp<AbstractTransfer>() {
            @Mock
            public void waitForCompletion()
                    throws AmazonClientException, AmazonServiceException, InterruptedException {

            }
        };

        carbonS3OutputStream.close();
        Field closedField = carbonS3OutputStream.getClass().getDeclaredField("closed");
        closedField.setAccessible(true);
        boolean closed = (boolean) closedField.get(carbonS3OutputStream);
        assertTrue(closed);
    }

    @Test(expected = IOException.class)
    public void closeTestExceptionCase() throws IOException, NoSuchFieldException, IllegalAccessException {
        CarbonS3FileSystem.CarbonS3OutputStream carbonS3OutputStream = new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, true, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID");
        new MockUp<TransferManager>() {
            @Mock
            public Upload upload(final PutObjectRequest putObjectRequest)
                    throws AmazonServiceException, AmazonClientException {
                return new UploadImpl("upload object", new TransferProgress(), new ProgressListenerChain(), null);
            }
        };

        new MockUp<AbstractTransfer>() {
            @Mock
            public void waitForCompletion()
                    throws AmazonClientException, AmazonServiceException, InterruptedException, IOException {
                throw new AmazonClientException("Exception Occured");
            }
        };

        carbonS3OutputStream.close();
    }

    @Test(expected = InterruptedIOException.class)
    public void closeTestInterruptedExceptionCase() throws IOException, NoSuchFieldException, IllegalAccessException {
        CarbonS3FileSystem.CarbonS3OutputStream carbonS3OutputStream = new CarbonS3FileSystem.CarbonS3OutputStream(new AmazonS3Client(), new TransferManagerConfiguration(), "host", "key", f, true, CarbonS3FileSystem.CarbonS3SseType.S3, "keyID");
        new MockUp<TransferManager>() {
            @Mock
            public Upload upload(final PutObjectRequest putObjectRequest)
                    throws AmazonServiceException, AmazonClientException {
                return new UploadImpl("upload object", new TransferProgress(), new ProgressListenerChain(), null);
            }
        };

        new MockUp<AbstractTransfer>() {
            @Mock
            public void waitForCompletion()
                    throws AmazonClientException, AmazonServiceException, InterruptedException, IOException {
                throw new InterruptedException("Exception Occured");
            }
        };

        carbonS3OutputStream.close();
    }


}
