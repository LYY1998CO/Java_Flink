package com.yyli.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustomerTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //DataStreamSource<Event> customeStream = env.addSource(new clickSource());
        DataStreamSource<Integer> customeStream = env.addSource(new ParallelCustomSource()).setParallelism(2);
        customeStream.print();
        env.execute();
    }
}
