package com.yyli.chapter06;

import com.yyli.chapter05.Event;
import com.yyli.chapter05.clickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//自定义周期性生成水位线
public class WaterMarkOnPeriod {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new clickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }
    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event>{

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomerWatermarkGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp;
                }
            };
        }
    }

    public static class  CustomerWatermarkGenerator implements WatermarkGenerator<Event>{
        private Long delaytime=5000L;//延迟时间
        private Long maxts=Long.MIN_VALUE+delaytime+1L;//观察到的最大时间戳
        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            maxts=Math.max(event.timestamp,maxts);//更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //发射水位线
            watermarkOutput.emitWatermark(new Watermark(maxts-delaytime-1L));
        }
    }

}
