package com.yyli.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//taskslot 任务槽 TODO
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置执行模式
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 2.读取文件
        DataStreamSource<String> Streamsource = env.readTextFile("Input/words.txt");
        //转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = Streamsource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //分组
        KeyedStream<Tuple2<String, Long>, String> stream = wordAndOneTuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = stream.sum(1);
        //打印输出
        sum.print();
        //启动执行
        env.execute();
    }
}
