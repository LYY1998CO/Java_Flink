package com.yyli.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Parameter;
import java.util.Collection;

// 无界的流
public class UnBoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从参数中提取主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // 通过配置参数来进行控制
        String hostname=parameterTool.get("host");
        Integer port=parameterTool.getInt("port");
        // 读取文件流
//        DataStreamSource<String> lineStream = env.socketTextStream("192.168.88.100", 7777);
        DataStreamSource<String> lineStream = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //分组
        KeyedStream<Tuple2<String, Long>, String> stream = wordAndOneTuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = stream.sum(1);
        //打印输出(sink)
        sum.print();
        //启动执行
        env.execute();
    }
}
