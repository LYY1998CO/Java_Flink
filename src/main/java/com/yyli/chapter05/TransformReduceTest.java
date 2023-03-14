package com.yyli.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./pro?id=100", 3000L),
                new Event("Bob", "./pro?id=1", 3300L),
                new Event("Alice", "./pro?id=200", 3200L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./pro?id=2", 3800L),
                new Event("Bob", "./pro??id=3", 4200L)
        );

        //1.统计每个用户量的访问频次(类似于wordcount)
        /*
        keyBy(data -> data.f0)中的data.f0其实就是指的是二元组中的第一个值,用他来进行分组
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksbyuser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {


                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                    }
                });
        //根据当前的个数选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksbyuser.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return stringLongTuple2.f1 > t1.f1 ? stringLongTuple2 : t1;
            }
        });
        result.print();
        env.execute();
    }
}
