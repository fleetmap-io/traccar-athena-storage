package org.fleetmap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.hadoop.ParquetWriter;
import org.traccar.model.Position;

import java.util.List;

public class ParquetWriterUtil {

    private static final Schema SCHEMA = new Schema.Parser().parse("""
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

    public static void writeToS3(List<Position> positions) {
        if (positions == null || positions.isEmpty()) {
            return;
        }

        try {
            Position sample = positions.getFirst();

            long deviceId = sample.getDeviceId();
            long shard = deviceId / 10;

            // Partitioned path format: deviceid_shard=xxx/fixdate=yyyy-MM-dd/
            String fixDate = new java.text.SimpleDateFormat("yyyy-MM-dd").format(sample.getFixTime());
            String key = String.format("deviceid_shard=%d/fixdate=%s/%d.parquet", shard, fixDate, deviceId);

            OutputFile outputFile = new S3OutputFile(key);
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                    .withSchema(SCHEMA)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build();

            for (Position pos : positions) {
                GenericRecord record = new GenericData.Record(SCHEMA);
                record.put("id", pos.getId());
                record.put("deviceId", pos.getDeviceId());
                record.put("latitude", pos.getLatitude());
                record.put("longitude", pos.getLongitude());
                record.put("fixTime", pos.getFixTime().getTime());
                record.put("speed", pos.getSpeed());
                writer.write(record);
            }

            writer.close();
        } catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }
}
