package com.ververica

import com.ververica.models.{Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration

/**
 * Use Flink's state and time to perform record deduplication
 *
 * Prerequisites:
 *  - Kafka/Zookeeper started: docker-compose up -d
 *
 * How to run:
 *  - Run this example
 *  - Periodically generate new transactions/ with: [[FillKafkaWithTransactions]]
 *  - The retentionTime is set to 1 minute, so re-generated transactions during this time do not show in the log
 *
 * Similar to: [[FraudDetectionJob]]
 *
 * */
@main def example5 =
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

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

  transactionStream
    // Select the attribute to dedupe
    .keyBy((t: Transaction) => t.t_id)
    // Add an operator that applies a function to each partitioned element in the stream
    .process(new DataStreamDeduplicate)
    .executeAndCollect
    .forEachRemaining(each => logger.info("After deduplication: {}", each))

/**
 * Business logic for deduplication
 *
 */
class DataStreamDeduplicate
    extends KeyedProcessFunction[Long, Transaction, Transaction]:
  // use Flink's managed keyed state
  var seen: ValueState[Transaction] = _

  val retentionTimeMinutes = 1

  override def open(parameters: Configuration): Unit =
    seen = getRuntimeContext.getState(
      new ValueStateDescriptor("seen", classOf[Transaction])
    )

  @throws[Exception]
  override def processElement(
                               transaction: Transaction,
                               context: KeyedProcessFunction[Long, Transaction, Transaction]#Context,
      out: Collector[Transaction]
  ): Unit =
    if (seen.value == null) {
      seen.update(transaction)
      // use timers to clean up state: This sets the "window to the past"
      context.timerService.registerProcessingTimeTimer(
        context.timerService.currentProcessingTime +
          // we'll keep each item for this time
          Duration.ofMinutes(retentionTimeMinutes).toMillis
      )
      out.collect(transaction)
    }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, Transaction, Transaction]#OnTimerContext,
      out: Collector[Transaction]
  ): Unit =
    seen.clear()
