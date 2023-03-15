package com.yyli.chapter06;

import com.yyli.chapter05.Event;
import com.yyli.chapter05.clickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;
//自定义数据源中发送水位线
public class WaterMarkOnCustomSource {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSourceWithWaterMark()).print();
        env.execute();
    }

    public static class ClickSourceWithWaterMark implements SourceFunction<Event>{
        private boolean running=true;
        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random=new Random();

            String[] userArr={"yyli","gychen","wpzhao"};

            String[] urlArr={"./home","./cart","./prod"};

            while (running){
                long currTs = Calendar.getInstance().getTimeInMillis(); //毫秒时间戳

                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];

                Event event=new Event(username,url,currTs);

                //使用collectWithTimestamp方法将数据发送出去,并指明数据中的时间戳字段
                sourceContext.collectWithTimestamp(event,event.timestamp);

                //发送水位线

                sourceContext.emitWatermark(new Watermark(event.timestamp-1L));

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }
}
