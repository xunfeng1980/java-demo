package com.coralmesh.pemja;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class Main {
    public static void main(String[] args) {
        PythonInterpreterConfig config = PythonInterpreterConfig.newBuilder().setPythonExec("/Users/lux/opt/miniconda3/envs/py39/bin/python") // specify python exec
                .build();


        try (PythonInterpreter interpreter = new PythonInterpreter(config)) {
            // set & get
            interpreter.set("a", 12345);
            interpreter.get("a"); // Object
            Object a = interpreter.get("a", Object.class); // int
            System.out.println(a);
            // exec & eval
            interpreter.exec("print(a)");
            interpreter.exec("import pandas as pd\n" + "\n" + "df = pd.read_csv('https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv')\n" + "\n" + "print(df.to_string())");

            interpreter.exec("import torch\n" + "x = torch.rand(5, 3)\n" + "print(x)");
            interpreter.exec("from transformers import pipeline\n" + "unmasker = pipeline('fill-mask', model='albert-base-v2')\n" + "res = unmasker(\"Hello I'm a [MASK] model.\")\n" + "print(res)");
        }

    }
}