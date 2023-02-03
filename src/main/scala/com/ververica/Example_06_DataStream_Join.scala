package com.ververica

import com.ververica.data.ExampleData
import com.ververica.models.{Customer, Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{ConfigConstants, Configuration, RestOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.jdk.CollectionConverters.*
import util.{LogMapFunction, MyCoMapFunction}

import scala.sys.process.*

/**
 * Same as [[example5]] but also shows Flink's state to perform record joining (transactions/customers)
 *
 * Doc:
 * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/process_function/#low-level-joins
 *
 */
@main def example6(): Unit =
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val port = 8082
  val config = new Configuration()
  config.setInteger(RestOptions.PORT, port)
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
  openWebUI(s"http://localhost:$port")

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
      // For debugging we want to log the elements before they get joined
      .map((each: Transaction) =>
        logger.info(s"After deduplication: $each")
        each)

  // join transactions (from Kafka) and customers (local)
  env
    .fromElements(ExampleData.customers: _*)
    .connect(deduplicatedTransactionStream)
    .keyBy((c: Customer) => c.c_id, (t: Transaction) => t.t_customer_id)
    .process(new JoinCustomersWithTransaction)
    .executeAndCollect("Example_06_DataStream_Join")
    .forEachRemaining(each => logger.info("After deduplication and joining: {}", each))

class JoinCustomersWithTransaction
  extends KeyedCoProcessFunction[Long, Customer, Transaction, String]:
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

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

    if txs.nonEmpty then
      join(collector, in1, txs)
    else logger.info("Buffer transactions, the customer stream is not ready yet")

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
    else logger.info("Buffer customer, the transaction stream is not ready yet")

  private def join(
                    out: Collector[String],
                    c: Customer,
                    txs: LazyList[Transaction]
                  ): Unit =
    logger.info(s"Joining: ${txs.length} transaction(s)")
    txs.foreach(t => out.collect(s"Transaction(time:${t.t_time}, amount:${t.t_amount})|Customer(id:${c.c_id} name:${c.c_name})"))
    // Now that we have found all transactions for this customer, clear the shared state
    transactions.clear()

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

def openWebUI(url: String) = {
  val os = System.getProperty("os.name").toLowerCase

  val newThread = new Thread(() => {
    Thread.sleep(5000)
    if (os == "mac os x") Process(s"open $url").!
    else if (os == "windows 10") Seq("cmd", "/c", s"start $url").!
  })
  newThread.start()
}