package com.cs.rfq.decorator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class RfqDataLoader {
    private final static Logger log = LoggerFactory.getLogger(RfqDataLoader.class);

    public Dataset<Row> loadRfq(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema =
                new StructType(new StructField[]{
                        new StructField("id", StringType, false, Metadata.empty()),
                        new StructField("traderId", StringType, false, Metadata.empty()),
                        new StructField("entityId", LongType, false, Metadata.empty()),
                        new StructField("instrumentId", LongType, false, Metadata.empty()),
                        new StructField("qty", LongType, false, Metadata.empty()),
                        new StructField("price", DoubleType, false, Metadata.empty()),
                        new StructField("side", StringType, false, Metadata.empty())
                });

        //TODO: load the trades dataset
        Dataset<Row> rfqs = session.read().schema(schema).json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        log.info(String.format("Initialised service with %s rfq records, using schema:", rfqs.count()));
        rfqs.printSchema();

        return rfqs;
    }
}
