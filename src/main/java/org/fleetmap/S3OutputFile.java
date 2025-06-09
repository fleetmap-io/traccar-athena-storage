package org.fleetmap;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class S3OutputFile implements OutputFile {

    private static final S3Client s3Client = S3Client.builder()
            //.region(Region.of("us-east-1")) // Set your region
            //.credentialsProvider(DefaultCredentialsProvider.create())
            // .endpointOverride(URI.create("http://localhost:4566")) // Uncomment for local testing (e.g., LocalStack)
            .build();

    String bucket = Config.getBucket();
    private final String key;

    public S3OutputFile(String key) {
        this.key = key;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) {
                outputStream.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                outputStream.write(b, off, len);
                position += len;
            }

            @Override
            public void close() throws IOException {
                outputStream.close();
                byte[] bytes = outputStream.toByteArray();

                s3Client.putObject(
                        PutObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .build(),
                        RequestBody.fromBytes(bytes)
                );
            }
        };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }
}
