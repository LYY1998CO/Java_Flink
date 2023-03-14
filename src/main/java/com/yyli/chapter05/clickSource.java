package com.yyli.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;


public class clickSource implements SourceFunction<Event> {
    //声明一个标志
    private Boolean running=true;
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        //随机生成数据
        Random random=new Random();
        //定义字段选取的数据集
        String [] users={"yli","gychen","wpzhao"};
        String [] urls={"./home","./carts","./pro","./prod?id=100","./prod?id=10"};

        //循环生成数据
        while (running){
            String user=users[random.nextInt(users.length)];
            String url=urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user,url,timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
