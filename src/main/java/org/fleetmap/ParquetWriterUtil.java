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
        { "name": "deviceId", "type": "string" },
        { "name": "latitude", "type": "double" },
        { "name": "longitude", "type": "double" },
        { "name": "fixTime", "type": { "type": "long", "logicalType": "timestamp-millis" } },
        { "name": "speed", "type": "double" }
      ]
    }
    """);

    public static void writeToS3(List<Position> positions) {
        try {
            OutputFile outputFile = new S3StreamingOutputFile("your-key.parquet");
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
            e.printStackTrace();
        }
    }
}
