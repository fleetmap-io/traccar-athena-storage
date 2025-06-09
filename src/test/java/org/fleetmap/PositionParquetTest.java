package org.fleetmap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.traccar.model.Position;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import static org.fleetmap.Athena.waitForQueryToComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PositionParquetTest {

    private static final long DEVICE_ID = 0;
    private final String testDate = new SimpleDateFormat("yyyy-MM-dd").format(Date.from(Instant.now()));

    @BeforeEach
    public void setup() {
        deleteKeysForTest(DEVICE_ID, testDate);
    }

    public static void deleteKeysForTest(long deviceId, String date) {
        long shard = deviceId / 10;
        String prefix = String.format("deviceid_shard=%d/date=%s/", shard, date);

        try (S3Client s3 = S3Client.create()) {

            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(Config.getBucket())
                    .prefix(prefix)
                    .build();

            ListObjectsV2Response listRes = s3.listObjectsV2(listReq);
            List<S3Object> objects = listRes.contents();

            if (!objects.isEmpty()) {
                DeleteObjectsRequest deleteReq = DeleteObjectsRequest.builder()
                        .bucket(Config.getBucket())
                        .delete(Delete.builder()
                                .objects(objects.stream()
                                        .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                                        .toList())
                                .build())
                        .build();

                s3.deleteObjects(deleteReq);
            }
        }
    }

    @Test
    public void testS3Writer() throws InterruptedException {
        Athena.createTableIfNotExists();

        Position position = new Position();
        position.setDeviceId(DEVICE_ID);
        position.setLatitude(38.7169);
        position.setLongitude(-9.1399);
        position.setSpeed(66.0);
        position.setFixTime(java.util.Date.from(Instant.now()));

        try (S3 writer = new S3()) {
            writer.write(position);
            writer.write(position);
        }

        String qId = Athena.startQueryExecution(String.format("""
            Select * from %s where deviceid_shard='0' and date='%s' and deviceId=%d
            """, Config.getTable(), testDate, DEVICE_ID
        ));
        waitForQueryToComplete(qId);
        List<Position> positions = Athena.getResult(qId);
        assertEquals(2, positions.size());
        assertEquals(position.getLatitude(), positions.getFirst().getLatitude());
        assertEquals(position.getLongitude(), positions.getFirst().getLongitude());
        assertEquals(position.getDeviceId(), positions.getFirst().getDeviceId());
    }
}
