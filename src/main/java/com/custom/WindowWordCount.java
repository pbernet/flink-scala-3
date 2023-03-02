package com.custom;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

/**
 * Windowing with processing time.
 * Similar to akka-streams `groupedWithin` operator
 *
 * How to use:
 *  - Start the input tcp stream from a terminal eg: nc -lk 1000
 *  - Run this app
 *  - Type sentences into the terminal
 *  - Watch the word count in the log
 *
 * Doc:
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/overview/
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 1000)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0) //keyBy word
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .sum(1);  // emit

        dataStream.print();

        env.execute("WindowWordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}