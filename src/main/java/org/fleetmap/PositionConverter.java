package org.fleetmap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.traccar.model.Position;
import software.amazon.awssdk.services.athena.model.Datum;

import java.util.List;

public class PositionConverter {

    public static final Schema SCHEMA = new Schema.Parser().parse("""
    {
      "type": "record",
      "name": "Position",
      "fields": [
        { "name": "id", "type": "long" },
        { "name": "deviceId", "type": "long" },
        { "name": "latitude", "type": "double" },
        { "name": "longitude", "type": "double" },
        { "name": "fixTime", "type": { "type": "long", "logicalType": "timestamp-millis" } },
        { "name": "speed", "type": "double" }
      ]
    }
    """);

    public static Position toPosition(List<Datum> data) {
        Position pos = new Position();
        pos.setId(Long.parseLong(data.get(0).varCharValue()));
        pos.setDeviceId(Long.parseLong(data.get(1).varCharValue()));
        pos.setFixTime(new java.util.Date(java.sql.Timestamp.valueOf(data.get(2).varCharValue()).getTime()));
        pos.setLatitude(Double.parseDouble(data.get(3).varCharValue()));
        pos.setLongitude(Double.parseDouble(data.get(4).varCharValue()));
        pos.setSpeed(Double.parseDouble(data.get(5).varCharValue()));
        return pos;
    }

    public static GenericRecord toGenericRecord(Position pos) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", pos.getId());
        record.put("deviceId", pos.getDeviceId());
        record.put("latitude", pos.getLatitude());
        record.put("longitude", pos.getLongitude());
        record.put("fixTime", pos.getFixTime().getTime());
        record.put("speed", pos.getSpeed());
        return record;
    }
}
