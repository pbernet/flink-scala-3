package com.ververica

import com.ververica.data.ExampleData
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

/**
 * Utility to write example "business transactions" into the Kafka topic "transactions"
 * 
 */
@main def fillKafkaWithTransactions =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv = StreamTableEnvironment.create(env)
  val transactionStream = env.fromElements(ExampleData.transaction: _*)
  tableEnv
    .fromDataStream(transactionStream)
    .executeInsert(
      TableDescriptor
        .forConnector("upsert-kafka")
        .schema(
          Schema
            .newBuilder()
            .column("t_time", DataTypes.TIMESTAMP_LTZ(3))
            .column("t_id", DataTypes.BIGINT().notNull())
            .column("t_customer_id", DataTypes.BIGINT().notNull())
            .column("t_amount", DataTypes.BIGINT())
            .primaryKey("t_id")
            .watermark("t_time", "t_time - INTERVAL '10' SECONDS")
            .build()
        )

        .option("key.format", "json")
        .option("value.format", "json")
        .option("value.json.timestamp-format.standard", "ISO-8601")
        .option("topic", "transactions")
        .option("properties.bootstrap.servers",  "localhost:29092")
        .build()
    )
