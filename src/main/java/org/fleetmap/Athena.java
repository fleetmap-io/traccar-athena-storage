package org.fleetmap;
import org.traccar.model.Position;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.ArrayList;
import java.util.List;

import static org.fleetmap.PositionConverter.toPosition;

public class Athena {
    private static final AthenaClient athena = AthenaClient.create();
    private static final QueryExecutionContext context = QueryExecutionContext.builder()
            .database(Config.getDatabase())
            .build();

    private static final ResultConfiguration resultConfig = ResultConfiguration.builder()
            .outputLocation("s3://" + Config.getBucket() + "/query_results")
            .build();

    public static String startQueryExecution(String query) {
        System.out.println(query);
        StartQueryExecutionRequest request = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(context)
                .resultConfiguration(resultConfig)
                .build();
        StartQueryExecutionResponse response = athena.startQueryExecution(request);
        return response.queryExecutionId();
    }

    public static void waitForQueryToComplete(String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();

        while (true) {
            GetQueryExecutionResponse response = athena.getQueryExecution(request);
            QueryExecutionState state = response.queryExecution().status().state();
            if (state == QueryExecutionState.SUCCEEDED) {
                return;
            } else if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED) {
                throw new RuntimeException("Athena query failed: " + response.queryExecution().status().stateChangeReason());
            }
            Thread.sleep(100);
        }
    }

    public static void createTableIfNotExists() throws InterruptedException {
        try (S3Client s3 = S3Client.builder().build()) {
            String bucket = Config.getBucket();
            boolean exists = s3.listBuckets().buckets().stream()
                    .anyMatch(b -> b.name().equals(bucket));
            if (!exists) {
                CreateBucketRequest.Builder requestBuilder = CreateBucketRequest.builder()
                        .bucket(bucket);

                requestBuilder.createBucketConfiguration(CreateBucketConfiguration.builder().build());
                s3.createBucket(requestBuilder.build());
                System.out.println("Created bucket: " + bucket);
            } else {
                System.out.println("Bucket already exists: " + bucket);
            }
        }
        String queryExecutionId = startQueryExecution("CREATE DATABASE IF NOT EXISTS " + Config.getDatabase());
        waitForQueryToComplete(queryExecutionId);
        queryExecutionId = startQueryExecution(generateCreateTableStatement(Config.getBucket(), Config.getTable()));
        waitForQueryToComplete(queryExecutionId);
    }

    public static String generateCreateTableStatement(String bucketName, String tableName) {
        return String.format("""
            CREATE EXTERNAL TABLE IF NOT EXISTS %s (
              id BIGINT,
              deviceid BIGINT,
              fixtime TIMESTAMP,
              latitude DOUBLE,
              longitude DOUBLE,
              speed DOUBLE
            )
            PARTITIONED BY (
              deviceid_shard STRING,
              date STRING
            )
            STORED AS PARQUET
            LOCATION 's3://%s/'
            TBLPROPERTIES (
              'projection.enabled' = 'true',
              'projection.deviceid_shard.type' = 'injected',
              'projection.date.type' = 'date',
              'projection.date.range' = '2000-01-01,NOW',
              'projection.date.format' = 'yyyy-MM-dd',
              'storage.location.template' = 's3://%s/deviceid_shard=${deviceid_shard}/date=${date}/'
            );
        """, tableName, bucketName, bucketName);
    }

    public static List<Position> getResult(String queryExecutionId) {
        GetQueryResultsIterable resultsIterable;
        try (AthenaClient client = AthenaClient.create()) {
            GetQueryResultsRequest resultsRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(queryExecutionId)
                    .build();

            resultsIterable = client.getQueryResultsPaginator(resultsRequest);
            List<Position> positions = new ArrayList<>();
            boolean isFirstRow = true;
            for (GetQueryResultsResponse results : resultsIterable) {
                for (Row row : results.resultSet().rows()) {
                    if (isFirstRow) {
                        isFirstRow = false;
                        continue;
                    }
                    positions.add(toPosition(row.data()));
                }
            }
            return positions;
        }
    }
}
