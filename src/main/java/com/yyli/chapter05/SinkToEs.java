package com.yyli.chapter05;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.server.Request;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkToEs {
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
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.88.101", 9200));

        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {

            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put(event.user,event.url);
                //构建一个IndexRequest
                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type")
                        .source(map);
                requestIndexer.add(request);
            }
        };
        //写入ES
        stream.addSink(new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();
    }

}