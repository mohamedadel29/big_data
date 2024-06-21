package com.strucuredstreaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class ReadFromKafkaToKafka {

    private static Map<String, Double> accumulatedProfitMap = new HashMap<>();

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession session = SparkSession.builder()
                .appName("ReadFromKafka")
                .master("local[1]")
                .getOrCreate();

        // Define the schema to match your JSON data
        StructType schema = new StructType()
                .add("_id", DataTypes.StringType)
                .add("name", DataTypes.StringType)
                .add("quantity", DataTypes.IntegerType)
                .add("sold", DataTypes.IntegerType)
                .add("price", DataTypes.DoubleType);

        // Read data from Kafka and apply schema
        Dataset<Row> readData = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "orders")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as json");

        // Parse the JSON data and apply the schema
        Dataset<Row> kafkaData = readData.select(from_json(col("json"), schema).as("data"))
                .select("data.*");

        StreamingQuery query = kafkaData.writeStream()
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("90 seconds"))
                .foreachBatch((batchDF, batchId) -> {
                    // Logging to verify data in batch
                    System.out.println("Received batch with " + batchDF.count() + " records.");

                    // Check if the batchDF is not empty
                    if (!batchDF.isEmpty()) {
                        // Remove duplicates based on the unique identifier '_id'
                        Dataset<Row> deduplicatedBatchDF = batchDF.dropDuplicates("_id");

                        // Add more logging statements here to debug calculations and aggregations
                        System.out.println("Performing calculations and aggregations on batch data.");

                        // Calculate profit for each row in the batch
                        Dataset<Row> results = deduplicatedBatchDF
                                .withColumn("profit", col("price").multiply(col("sold")))
                                .withColumn("valid_items", col("quantity"))
                                .groupBy("name")
                                .agg(
                                        sum("profit").alias("total_profit"),
                                        max("valid_items").alias("valid_items"),
                                        max("sold").alias("sold")
                                );

                        // Debugging: Show the results before writing
                        results.show();

                        // Collect the DataFrame to a list of JSON strings
                        List<String> jsonArray = results.toJSON().collectAsList();

                        // Combine the JSON strings into a single JSON array string
                        String jsonOutput = jsonArray.stream().collect(Collectors.joining(",", "[", "]"));

                        // Write the JSON array string to the output file
                        String outputPath = String.format("G://hand-made//public//output_1.json", batchId);
                        try (FileWriter file = new FileWriter(outputPath, false)) {
                            file.write(jsonOutput);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    } else {
                        // Handle the case when there is no data in the batch
                        System.out.println("No data in the batch.");
                    }
                })
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
