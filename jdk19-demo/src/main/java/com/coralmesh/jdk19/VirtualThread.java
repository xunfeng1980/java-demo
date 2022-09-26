package com.coralmesh.jdk19;

public class VirtualThread {

    Runnable runnable = () -> {
        System.out.println(", I am a virtual thread: " + Thread.currentThread().isVirtual());
    };

    public static void main(String... args) {
        new VirtualThread().run();
    }

    private void run() {
        Thread.ofVirtual().start(runnable);
        Thread.ofPlatform().start(runnable);
    }
}