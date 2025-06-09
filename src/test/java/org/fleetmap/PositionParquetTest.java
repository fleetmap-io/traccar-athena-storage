package org.fleetmap;

import org.junit.jupiter.api.Test;
import org.traccar.model.Position;

import java.time.Instant;

import static org.fleetmap.Athena.waitForQueryToComplete;

public class PositionParquetTest {

    @Test
    public void testS3Writer() throws InterruptedException {
        Position position = new Position();
        position.setDeviceId(424);
        position.setLatitude(38.7169);
        position.setLongitude(-9.1399);
        position.setSpeed(66.0);
        position.setFixTime(java.util.Date.from(Instant.now()));

        S3 writer = new S3(1);
        writer.write(position);

        Athena.createTableIfNotExists();

        waitForQueryToComplete(Athena.startQueryExecution("Select * from " + Config.getTable() + " where deviceid_shard=424"));

    }
}
