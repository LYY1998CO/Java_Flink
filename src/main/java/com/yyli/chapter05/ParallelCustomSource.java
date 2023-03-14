package com.yyli.chapter05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

//实现自定义的并行sourcefunction
public class ParallelCustomSource implements ParallelSourceFunction<Integer> {
    private Boolean running=true;
    private Random random=new Random();
    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running){
            sourceContext.collect(random.nextInt());
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
