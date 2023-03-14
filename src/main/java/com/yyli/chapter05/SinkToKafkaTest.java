package com.yyli.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.从kafka中读取数据
        Properties p =new Properties();
        p.setProperty("bootstrap.servers","192.168.88.100:9092");

        DataStreamSource<String> kafka_stream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), p));

        //2.用Flink进行转换处理
        SingleOutputStreamOperator<String> result = kafka_stream.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                String[] fileds = s.split(",");
                return new Event(fileds[0].trim(), fileds[1].trim(), Long.valueOf(fileds[2].trim())).toString();
            }
        });

        //3.将结果数据写入kafka
        result.addSink(new FlinkKafkaProducer<String>("192.168.88.100:9092","events",new SimpleStringSchema()));

        env.execute();
    }
}
