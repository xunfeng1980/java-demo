package com.coralmesh.temporal;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface Format {

    @ActivityMethod
    String composeGreeting(String name);
}

class FormatImpl implements Format {

    @Override
    public String composeGreeting(String name) {
        return "Hello " + name + "!";
    }
}
interface Shared {

    String HELLO_WORLD_TASK_QUEUE = "HELLO_WORLD_TASK_QUEUE";
}