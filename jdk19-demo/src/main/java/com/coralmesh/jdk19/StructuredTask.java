package com.coralmesh.jdk19;


import jdk.incubator.concurrent.StructuredTaskScope;

public class StructuredTask {
    public static void main(String[] args) throws Exception {
        try (var scope = new StructuredTaskScope<Void>()) {
            scope.fork(() -> delayPrint(1000, "Hello,"));
            scope.fork(() -> delayPrint(2000, "World!"));
            scope.fork(() -> mockException(500, "mockException!"));
            scope.join();
        }
        System.out.println("Done!");
    }

    private static Void delayPrint(long delay, String message) throws Exception {
        Thread.sleep(delay);
        System.out.println(message);
        return null;
    }

    private static Void mockException(long delay, String message) throws Exception {
        Thread.sleep(delay);
        System.out.println(message);
        throw new Exception("Error");
    }
}
