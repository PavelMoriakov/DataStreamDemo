package com.epam.sink;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PersonFileSink extends RichSinkFunction<Tuple1<String>> {
    @Override
    public void invoke(Tuple1<String> value, Context context) throws Exception {

    }
}
