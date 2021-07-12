package com.pluralsight.streaming;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/execution_mode/
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<String> text = env.readTextFile("wasbs://flink@tmaflinkstorage01.blob.core.windows.net/input/");
//        DataStream<String> text = env.readTextFile("test.txt");

        DataStream<Tuple2<String, Integer>> dataStream = text
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);

//          dataStream.print();
        dataStream.writeAsText("wasbs://flink@tmaflinkstorage01.blob.core.windows.net/output/output.txt");


        env.execute("Text WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
