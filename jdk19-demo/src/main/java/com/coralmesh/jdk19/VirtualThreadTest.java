package com.coralmesh.jdk19;

public class VirtualThreadTest {

    public static void main(String... args) throws InterruptedException {
        new VirtualThreadTest().run();
    }

    private void run() throws InterruptedException {
        Runnable runnable = () -> System.out.printf("I am a virtual thread: %s ,threadId: %d \n", Thread.currentThread().isVirtual(), Thread.currentThread().threadId());
        Thread vt = Thread.ofVirtual().start(runnable);
        Thread pt = Thread.ofPlatform().start(runnable);
        vt.join();
        pt.join();
    }
}