package com.coralmesh.datasketches;

import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

// simplified file operations and no error handling for clarity
public class HllExample {

    public static void main(String[] args) throws Exception {
        final int lgK = 10;

        // this section generates two sketches with some overlap and serializes them into files
        {
            // 100000 unique keys
            HllSketch sketch1 = new HllSketch(lgK);
            for (int key = 0; key < 100000; key++) sketch1.update(key);
            FileOutputStream out1 = new FileOutputStream("HllSketch1.bin");
            out1.write(sketch1.toCompactByteArray());
            out1.close();

            // 100000 unique keys
            HllSketch sketch2 = new HllSketch(lgK);
            for (int key = 50000; key < 150000; key++) sketch2.update(key);
            FileOutputStream out2 = new FileOutputStream("HllSketch2.bin");
            out2.write(sketch2.toCompactByteArray());
            out2.close();
        }

        // this section deserializes the sketches, produces union and prints the results
        {
            FileInputStream in1 = new FileInputStream("HllSketch1.bin");
            byte[] bytes1 = new byte[in1.available()];
            in1.read(bytes1);
            in1.close();
            HllSketch sketch1 = HllSketch.heapify(Memory.wrap(bytes1));

            FileInputStream in2 = new FileInputStream("HllSketch2.bin");
            byte[] bytes2 = new byte[in2.available()];
            in2.read(bytes2);
            in2.close();
            HllSketch sketch2 = HllSketch.heapify(Memory.wrap(bytes2));

            Union union = new Union(lgK);
            union.update(sketch1);
            union.update(sketch2);
            HllSketch unionResult = union.getResult(TgtHllType.HLL_4);

            // debug summary of the union result sketch
            System.out.println(unionResult.toString());

            System.out.println("Union unique count estimate: " + unionResult.getEstimate());
            System.out.println("Union unique count lower bound 95% confidence: " + unionResult.getLowerBound(2));
            System.out.println("Union unique count upper bound 95% confidence: " + unionResult.getUpperBound(2));
        }
    }
}