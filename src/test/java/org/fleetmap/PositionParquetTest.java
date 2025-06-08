package org.fleetmap;

import org.junit.jupiter.api.Test;
import org.traccar.model.Position;

import java.time.Instant;

public class PositionParquetTest {

    @Test
    public void testS3Writer() {
        // Sample position
        Position position = new Position();
        position.setDeviceId(42L);
        position.setLatitude(38.7169);
        position.setLongitude(-9.1399);
        position.setSpeed(66.0);
        position.setFixTime(java.util.Date.from(Instant.now()));

        S3Writer writer = new S3Writer(1);
        writer.write(position);

        System.out.println("✅ Successfully wrote position to S3");
    }
}
