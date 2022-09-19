package com.coralmesh.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

public class MySource implements SourceFunction<String> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            var stringList = new ArrayList<String>();
            for (int i = 0; i < 10; i++) {
                stringList.add(String.valueOf(i));
            }
            int size = stringList.size();
            int i = new Random().nextInt(size);
            sourceContext.collect(stringList.get(i));
            Thread.sleep(i * 100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
