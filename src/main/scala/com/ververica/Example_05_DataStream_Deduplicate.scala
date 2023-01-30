package com.ververica

import com.ververica.models.{Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
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
 *  - Periodically generate new transactions with: [[FillKafkaWithTransactions]]
 *  - With the retentionTime set to 1 minute, re-generated transactions during this time are deduped,
 *    hence do not show in the log
 *
 * Similar to: [[FraudDetectionJob]]
 *
 **/
@main def example5() =
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val transactionSource = KafkaSource
    .builder[Transaction]
    .setBootstrapServers("localhost:29092")
    .setTopics("transactions")
    .setStartingOffsets(OffsetsInitializer.earliest)
    .setValueOnlyDeserializer(new TransactionDeserializer)
    .build

  // We need the type to help IDEA with inspection
  val transactionStream: DataStreamSource[Transaction] = env.fromSource(
    transactionSource,
    WatermarkStrategy.noWatermarks,
    "Transactions"
  )

  transactionStream
    // Instead of t_id use t_customer_id and t_amount for deduping
    // Workaround with (anonymous) class because of "type trouble"
    .keyBy(
      new KeySelector[Transaction, (Long, Long)] {
        @throws[Exception]
        def getKey(t: Transaction) = (t.t_customer_id, t.t_amount)
      }
    )

    // Add an operator that applies a function to each partitioned element in the stream
    .process(new DataStreamDeduplicateMultipleKey)
    .executeAndCollect
    .forEachRemaining(each => logger.info("After deduplication: {}", each))


  /**
   * Business logic for deduplication (t_customer_id, t_amount)
   */
  class DataStreamDeduplicateMultipleKey
    extends KeyedProcessFunction[(Long, Long), Transaction, Transaction]:
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
                                 context: KeyedProcessFunction[(Long, Long), Transaction, Transaction]#Context,
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
                          ctx: KeyedProcessFunction[(Long, Long), Transaction, Transaction]#OnTimerContext,
                          out: Collector[Transaction]
                        ): Unit =
      seen.clear()