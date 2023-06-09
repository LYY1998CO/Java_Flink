package com.yyli.chapter05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToMysqlTest {
    public static void main(String[] args) throws Exception{
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

        stream.addSink(
                JdbcSink.sink(
                    "insert into clicks (user,url) values (?,?)",
                ((statement,event)->{
                    statement.setString(1,event.user);
                    statement.setString(2,event.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        )
        );

        env.execute();
    }
}

