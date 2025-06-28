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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PositionParquetTest {

    private static final long DEVICE_ID = 0;
    private final String testDate = new SimpleDateFormat("yyyy-MM-dd").format(Date.from(Instant.now()));

    @BeforeEach
    public void setup() {
        deleteKeysForTest(DEVICE_ID);
    }

    public static void deleteKeysForTest(long deviceId) {
        long shard = deviceId / 10;
        String prefix = String.format("deviceid_shard=%d", shard);

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

        Position position2 = new Position();
        position2.setDeviceId(DEVICE_ID);
        position2.setLatitude(37.7169);
        position2.setLongitude(-8.1399);
        position2.setSpeed(50.0);
        position2.setFixTime(java.util.Date.from(Instant.parse("2025-06-27T12:01:00Z")));


        try (S3 writer = new S3()) {
            writer.write(position);
            writer.write(position2);
        }

        String qId = Athena.startQueryExecution(String.format("""
            Select * from %s where deviceid_shard='0' and date='%s' and deviceId=%d
            """, Config.getTable(), testDate, DEVICE_ID
        ));
        waitForQueryToComplete(qId);
        List<Position> positions = Athena.getResult(qId);

        assertEquals(1, positions.size());
        assertEquals(position.getLatitude(), positions.getFirst().getLatitude());
        assertEquals(position.getLongitude(), positions.getFirst().getLongitude());
        assertEquals(position.getDeviceId(), positions.getFirst().getDeviceId());

        qId = Athena.startQueryExecution(String.format("""
            Select * from %s where deviceid_shard='0' and date between '2025-06-27' and '%s' and deviceId=%d
            """, Config.getTable(), testDate, DEVICE_ID
        ));
        waitForQueryToComplete(qId);
        positions = Athena.getResult(qId);
        assertTrue(positions.size() > 1);
        for (Position p : positions) {
            System.out.printf("Queried position: id=%d, lat=%.6f, lon=%.6f, time=%s%n",
                    p.getDeviceId(),
                    p.getLatitude(),
                    p.getLongitude(),
                    p.getFixTime()
            );
        }

        qId = Athena.startQueryExecution(String.format("""
            Select * from %s where deviceid_shard='0' and date between '2025-06-27' and '%s' and deviceId=%d
            AND fixTime BETWEEN TIMESTAMP '2025-06-27 12:01:00' AND TIMESTAMP '2025-06-27 23:59:59'
            """, Config.getTable(), testDate, DEVICE_ID
        ));
        waitForQueryToComplete(qId);
        positions = Athena.getResult(qId);
        assertEquals(1, positions.size());
        for (Position p : positions) {
            System.out.printf("Queried position: id=%d, lat=%.6f, lon=%.6f, time=%s%n",
                    p.getDeviceId(),
                    p.getLatitude(),
                    p.getLongitude(),
                    p.getFixTime()
            );
        }
    }
}
