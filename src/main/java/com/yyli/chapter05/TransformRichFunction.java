package com.yyli.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./pro?id=100", 3000L)
        );
//        stream.map(new MyRichMapper()).setParallelism(2).print().setParallelism(2);
        stream.map(new MyRichMapper()).setParallelism(2).print();
        stream.map(new RichMapFunction<Event, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("匿名内部类open生命周期被调用:"+getRuntimeContext().getIndexOfThisSubtask()+"号任务启动");
            }
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("匿名内部类close生命周期被调用:"+getRuntimeContext().getIndexOfThisSubtask()+"号任务结束");
            }
        }).setParallelism(2).print();
        env.execute();
    }
    //实现一个自定义的富函数类
    public static class MyRichMapper extends RichMapFunction<Event ,Integer>{
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用:"+getRuntimeContext().getIndexOfThisSubtask()+"号任务启动");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用:"+getRuntimeContext().getIndexOfThisSubtask()+"号任务结束");
        }
    }
}
