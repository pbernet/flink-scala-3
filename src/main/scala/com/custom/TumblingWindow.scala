package com.custom

import com.generators.{ShoppingCartEvent, ShoppingCartEventsGenerator}
import org.apache.flink.api.common.eventtime.*
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, DataStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, ProcessAllWindowFunction, ReduceApplyAllWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, OffsetDateTime, ZoneId}
import java.util
import scala.jdk.CollectionConverters.*
import scala.sys.process.{Process, *}

/**
 * Tumbling Session Window example based on code taken from the
 * "Rock the JVM Flink course".
 * No Scala API is used here, since it is deprecated
 *
 * Doc:
 * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows
 *
 */
@main def tumblingWindow() =
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val port = 8082
  val config = new Configuration()
  config.setInteger(RestOptions.PORT, port)
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
  // control how often Flink calls onPeriodicEmit
  env.getConfig.setAutoWatermarkInterval(1000L)
  openWebUI(s"http://localhost:$port")

  val shoppingCartEvents: DataStream[ShoppingCartEvent] = env.addSource(
    new ShoppingCartEventsGenerator(
      sleepMillisPerEvent = 100,
      batchSize = 10,
      baseInstant = Instant.now().minusSeconds(3600)
      //baseInstant = Instant.parse("2022-02-15T00:00:00.000Z")
    )
  )
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        // Once we get an event with time T, we will NOT accept further events with event time < T - 1s
        // Generic:
        //.forBoundedOutOfOrderness(java.time.Duration.ofMillis(1000L))
        // Customizable:
        .forGenerator(_ => new BoundedOutOfOrdernessGenerator(1000L))
        // Wire the field which contains the event time
        .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
          override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
            element.time.toEpochMilli
        })
    )

  val groupedEventsByWindow: AllWindowedStream[ShoppingCartEvent, TimeWindow] = shoppingCartEvents
    .map(event =>
      logger.info(s"Event: $event")
      event)
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  val countEventsByWindow: DataStream[String] = groupedEventsByWindow.apply(new CountByWindowAll())
  countEventsByWindow.map(window =>
    logger.info(s"$window")
    window)
    .setParallelism(10)
  env.execute("TumblingWindow")

/**
 * Is evaluated at "window closing time"
 */
class CountByWindowAll extends AllWindowFunction[ShoppingCartEvent, String, TimeWindow]() {
  override def apply(window: TimeWindow, input: java.lang.Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
    val eventCount = input.asScala.count(event => event.isInstanceOf[ShoppingCartEvent])
    out.collect(s"Window: [${tsToString(window.getStart)} - ${tsToString(window.getEnd)}] $eventCount events. Details: ${input.asScala.toList} ")
  }
}

/**
 * Custom watermarks
 * With every new MaxTimestamp, every new incoming element with event time < MaxTimestamp - maxDelay will be discarded
 *
 */
class BoundedOutOfOrdernessGenerator(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var currentMaxTimestamp: Long = 0L

  // Called on each element/event
  override def onEvent(event: ShoppingCartEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    currentMaxTimestamp = Math.max(currentMaxTimestamp, event.time.toEpochMilli)
  }

  // Called periodically, controlled by config setAutoWatermarkInterval
  // Up to us to emit a watermark at these times
  override def onPeriodicEmit(output: WatermarkOutput): Unit =
    val watermark = currentMaxTimestamp - maxDelay
    logger.info(s"Watermark is at: ${tsToString(watermark)}")
    output.emitWatermark(new Watermark(watermark))
}


def openWebUI(url: String): Unit = {
  val os = System.getProperty("os.name").toLowerCase

  val newThread = new Thread(() => {
    Thread.sleep(5000)
    if (os == "mac os x") Process(s"open $url").!
    else if (os == "windows 10") Seq("cmd", "/c", s"start $url").!
  })
  newThread.start()
}

def tsToString(ts: Long) = OffsetDateTime
  .ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("UTC"))
  .toLocalTime
  .format(DateTimeFormatter.ofPattern("HH:mm:ss"))