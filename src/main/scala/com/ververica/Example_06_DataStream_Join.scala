package com.ververica

import com.ververica.data.ExampleData
import com.ververica.models.{Customer, Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.jdk.CollectionConverters._
import util.{MyCoMapFunction, LogMapFunction}

/** Use Flink's state to perform record joining based on business requirements
 *
 * Doc:
 * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/process_function/#low-level-joins
 *
 * TODO
 * - Why do we have duplicate Transactions after JOIN for:
 *    Transaction(time:2021-10-14T17:04:00.001Z, amount:52)|Customer(id:12 name:Alice)
 *    Transaction(time:2021-10-14T17:04:00.001Z, amount:52)|Customer(id:12 name:Alice)
 *
 *    Transaction(time:2021-10-14T18:23:00.001Z, amount:22)|Customer(id:32 name:Bob)
 *    Transaction(time:2021-10-14T18:23:00.001Z, amount:22)|Customer(id:32 name:Bob)
 *
 */
@main def example6() =
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val env = StreamExecutionEnvironment.getExecutionEnvironment()

  // switch to batch mode on demand
  // env.setRuntimeMode(RuntimeExecutionMode.BATCH)

  val transactionSource: KafkaSource[Transaction] = KafkaSource
    .builder[Transaction]
    .setBootstrapServers("localhost:29092")
    .setTopics("transactions")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new TransactionDeserializer)
    // .setBounded(OffsetsInitializer.latest())
    .build()

  val deduplicatedTransactionStream: SingleOutputStreamOperator[Transaction] =
    env.fromSource(
      transactionSource,
      WatermarkStrategy.noWatermarks(),
      "Transactions")
      .keyBy((t: Transaction) => t.t_id)
      .process(new DataStreamDeduplicate)
      // We want to log the elements before it gets joined
      .map(new LogMapFunction[Transaction]())

  // join transactions (from Kafka) and customers (local)
  env
    .fromElements(ExampleData.customers: _*)
    .connect(deduplicatedTransactionStream)

    // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/user_defined_functions/
    // TODO Return type does not match keyBy - NO Support by IDE - very painful...
    //.map(new MyCoMapFunction())

    .keyBy((c: Customer) => c.c_id, (t: Transaction) => t.t_customer_id)
    .process(new JoinCustomersWithTransaction)
    .executeAndCollect
    .forEachRemaining(each => logger.info("After deduplication and joining: {}", each))

class JoinCustomersWithTransaction
  extends KeyedCoProcessFunction[Long, Customer, Transaction, String]:

  var customer: ValueState[Customer] = _
  var transactions: ListState[Transaction] = _

  override def open(parameters: Configuration): Unit =
    customer = getRuntimeContext.getState(
      new ValueStateDescriptor("customer", classOf[Customer])
    )
    transactions = getRuntimeContext.getListState(
      new ListStateDescriptor("transactions", classOf[Transaction])
    )

  override def processElement1(
                                in1: Customer,
                                context: KeyedCoProcessFunction[
                                  Long,
                                  Customer,
                                  Transaction,
                                  String
                                ]#Context,
                                collector: Collector[String]
                              ): Unit =
    customer.update(in1)
    val txs = transactions.get().asScala.to(LazyList)

    // TODO Why is this a 2.13 LazyList?
    if txs.nonEmpty then join(collector, in1, txs)

  override def processElement2(
                                in2: Transaction,
                                context: KeyedCoProcessFunction[
                                  Long,
                                  Customer,
                                  Transaction,
                                  String
                                ]#Context,
                                collector: Collector[String]
                              ): Unit =
    transactions.add(in2)
    val c = customer.value

    if c != null then
      join(collector, c, transactions.get().asScala.to(LazyList))

  private def join(
                    out: Collector[String],
                    c: Customer,
                    txs: LazyList[Transaction]
                  ): Unit =
    txs.foreach(t => out.collect(s"Transaction(time:${t.t_time}, amount:${t.t_amount})|Customer(id:${c.c_id} name:${c.c_name})"))

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
