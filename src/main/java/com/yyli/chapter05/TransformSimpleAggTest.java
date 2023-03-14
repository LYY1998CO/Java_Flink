package com.yyli.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./pro?id=100", 3000L),
                new Event("Bob", "./pro?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Alice", "./pro?id=200", 3200L),
                new Event("Bob", "./pro?id=2", 3800L),
                new Event("Bob", "./pro??id=3", 4200L)
        );
        //按键分组之后进行聚合,提取当前用户最近一次访问数据
        SingleOutputStreamOperator<Event> max = stream.keyBy(new KeySelector<Event, String>() {

            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp");
        max.print("1");
        //通过keyby这个方法进行分组操作:按照event对象中的user字段进行分组,然后通过max或者maxby函数对timestamp字段进行排序操作
        stream.keyBy(data -> data.user).maxBy("timestamp").print("maxBy");
        env.execute();

    }
}
