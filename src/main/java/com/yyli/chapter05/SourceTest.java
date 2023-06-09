package com.yyli.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");
        //2.从集合工读取数据
        ArrayList<Integer> nums=new ArrayList<>();
        nums.add(2);
        nums.add(5);
        nums.add(7);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();

        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./CART",10L));
        events.add(new Event("Cli","./pro",2000L));
        events.add(new Event("Tom","./mark",1300L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);
        // 3.从元素中读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("yyli", "./home", 1000L),
                new Event("ZKSUN", "./home", 1000L)
        );

        // 4.从socket文本流中 读取数据
        DataStreamSource<String> stream4 = env.socketTextStream("192.168.88.101", 7777);
//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");
//        stream3.print("3");
        // 5.从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.88.100:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafka_stream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
//        stream4.print("4");
        kafka_stream.print("kafka");
        env.execute();
    }
}
