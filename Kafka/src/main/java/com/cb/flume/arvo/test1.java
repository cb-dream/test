package com.cb.flume.arvo;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class test1  implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        byte b = body[0];
        Map<String, String> headers = event.getHeaders();
        if (b >= '0' && b <= '9'){
            // b为数字
            headers.put("type","number");
        }else if((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')){
            // b 为字母
            headers.put("type","letter");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new test1();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
