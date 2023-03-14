package com.yyli.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//扁平映射(1对多的形式)
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(new Event("yyli", "./home", 1000L),
                new Event("xlzhang", "./cart", 2000L),
                new Event("wpzhao", "./pro", 3000L));
        SingleOutputStreamOperator<String> result = stream.flatMap(new MyFlatMap());
        //2.使用匿名类实现
        SingleOutputStreamOperator<String> result2 = stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                if (event.user.equals("wpzhao"))
                    collector.collect(event.url);
                else if (event.user.equals("yyli"))
                {
                    collector.collect(event.user);
                    collector.collect(event.url);
                    collector.collect(event.timestamp.toString());
                }
//                collector.collect(event.user);
//                collector.collect(event.url);
//                collector.collect(event.timestamp.toString());
            }
        });
        //3.传入lambda表达式
        stream.flatMap((Event value,Collector<String> out) -> {
            if (value.user.equals("xlzhang"))
                out.collect(value.url);
            else if (value.user.equals("yyli")){
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {
        }).print("3");
        result.print("1");
        result2.print("2");
        env.execute();
    }
    //1.实现一个自定义的flatmapFunction
    public static class MyFlatMap implements FlatMapFunction<Event,String>{

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}
