package org.fleetmap;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class S3StreamingOutputFile implements OutputFile {
    private static final Properties properties = new Properties();
    static {
        try (InputStream input = S3StreamingOutputFile.class.getClassLoader().getResourceAsStream("athena-storage.properties")) {
            if (input != null) {
                properties.load(input);
            } else {
                throw new RuntimeException("config.properties not found in classpath");
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to load config.properties", ex);
        }
    }
    private static final S3Client s3Client = S3Client.builder()
            //.region(Region.of("us-east-1")) // Set your region
            //.credentialsProvider(DefaultCredentialsProvider.create())
            // .endpointOverride(URI.create("http://localhost:4566")) // Uncomment for local testing (e.g., LocalStack)
            .build();

    String bucket = (String)properties.get("s3.bucket");
    private final String key;

    public S3StreamingOutputFile(String key) {
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
            public void write(int b) throws IOException {
                outputStream.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
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
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
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
