package com.coralmesh.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;

public class Disruptor2P1CDemo {
    private static long st;
    private static long et;
    private static long PRODUCER_MSG_NO = 10_000_000;
    private static long sum = 0;
    public static void handleEvent(LongEvent event, long sequence, boolean endOfBatch) {
        long l = event.value;
        sum += l;
    }
    public static class LongEvent {
        private long value;
        public void set(long value) {
            this.value = value;
        }
    }

    static class ProducerThread extends Thread {
        int count = 0;
        Disruptor disruptor;
        public ProducerThread(Disruptor disruptor) {
            this.disruptor = disruptor;
        }
        @Override
        public void run() {
            RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
            while (count < PRODUCER_MSG_NO) {

                long sequence = ringBuffer.next();
                try {
                    LongEvent event = ringBuffer.get(sequence);
                    event.set(1);
                    count++;
                } finally {
                    ringBuffer.publish(sequence);
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        int bufferSize = 32768;
        Disruptor<LongEvent> disruptor =
                new Disruptor<>(LongEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);
        disruptor.handleEventsWith(Disruptor2P1CDemo::handleEvent);
        disruptor.start();
        Thread t1 = new ProducerThread(disruptor);
        Thread t2 = new ProducerThread(disruptor);

        //JVM warm up
        PRODUCER_MSG_NO = 100_000_000;
        t1.start();
        t2.start();
        t1.join();
        t2.join();


        //Start measuring
        PRODUCER_MSG_NO = 10_000_000;
        sum = 0;
        Thread tt1 = new ProducerThread(disruptor);
        Thread tt2 = new ProducerThread(disruptor);

        st = System.currentTimeMillis();
        tt1.start();
        tt2.start();

        //Waiting for producer threads
        tt1.join();
        tt2.join();
        et = System.currentTimeMillis();
        System.out.println(String.format("Sum %s, Process time: %s", sum, (et - st)));
    }
}