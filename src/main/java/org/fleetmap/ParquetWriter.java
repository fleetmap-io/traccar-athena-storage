package org.fleetmap;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.traccar.model.Position;

import java.io.IOException;
import java.util.List;

import static org.fleetmap.PositionConverter.SCHEMA;
import static org.fleetmap.PositionConverter.toGenericRecord;

public class ParquetWriter {
    public static void writeToS3(List<Position> positions, PartitionKey partitionKey) {
        String key = String.format("deviceid_shard=%d/date=%s/%d.parquet", partitionKey.getShard(), partitionKey.getDate(), System.currentTimeMillis());
        OutputFile outputFile = new S3OutputFile(key);
        try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (Position pos : positions) {
                writer.write(toGenericRecord(pos));
            }
        } catch (IOException ex) {
            //noinspection CallToPrintStackTrace
            ex.printStackTrace();
        }
    }
}
