package com.yyli.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("yyli", "./home", 1000L),
                new Event("xlzhang", "./cart", 2000L),
                new Event("wpzhao", "./pro", 3000L));
        SingleOutputStreamOperator<Event> result = stream.filter(new MyFilter());
        //2.使用匿名内部类实现
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                //可以写一个判断的逻辑或者简写都是可以的
                if (event.user.equals("yyli")){
                    return true;
                }
                return false;
//                return event.user.equals("wpzhao");
            }
        });
        //使用匿名函数(lambda函数)实现
        stream.filter(data->data.user.equals("xlzhang")).print("lambda,xlzhang");
        result2.print();
        env.execute();
    }
    //1.使用自定义类实现FilterFunction
    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event s) throws Exception {
            return s.user.equals("yyli");
        }
    }


}
