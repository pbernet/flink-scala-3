package com.ververica

import com.ververica.data.ExampleData
import com.ververica.models.{Customer, Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

/**
 * Use DataStream API connectors but deduplicate and join in Flink SQL
 * 
 * Similar to `TableDeduplicatedJoin`:
 * https://docs.immerok.cloud/docs/how-to-guides/development/joining-and-deduplicating-data
 *
 */
@main def example7() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // switch to batch mode on demand
  // env.setRuntimeMode(RuntimeExecutionMode.BATCH)
  val tableEnv = StreamTableEnvironment.create(env)

  val customerStream = env.fromElements(ExampleData.customers: _*)
  tableEnv.createTemporaryView("Customers", customerStream)

  val transactionSource = KafkaSource
    .builder[Transaction]
    .setBootstrapServers( "localhost:29092")
    .setTopics("transactions")
    .setStartingOffsets(OffsetsInitializer.earliest)
    .setValueOnlyDeserializer(new TransactionDeserializer)
    .build

  val transactionStream = env.fromSource(
    transactionSource,
    WatermarkStrategy.noWatermarks,
    "Transactions"
  )
  // seamlessly switch from DataStream to Table API
  tableEnv.createTemporaryView("Transactions", transactionStream)

  // use Flink SQL to do the heavy lifting
  // Note that DISTINCT is applied on all fields of a row
  // See example9 for a way to deduplicate on dedicated fields
  tableEnv.executeSql(
    """
      |SELECT c_name, CAST(t_amount AS DECIMAL(5, 2))
      |FROM Customers
      |JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id
      |""".stripMargin
  ).print()
