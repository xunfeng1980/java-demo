package com.coralmesh.jdk19;

import java.math.BigDecimal;

record Employee(String id, String name, String department) {
};

record Contractor(String id, String name, BigDecimal fee) {
};

public class RecordPatternMatchingClassic {
    public static void main(String... args) {
        new RecordPatternMatchingClassic().execute();
    }


    private void execute() {
        var john = new Employee("12", "John", "Auditing");
        var terry = new Contractor("c89", "Terry", BigDecimal.valueOf(105.50));

        process(john);
        process(terry);
    }


    private void process(Object o) {
        if (o instanceof Employee employee) {
            System.out.println(employee.name() + " => " + employee.department());
        }
        if (o instanceof Contractor contractor) {
            System.out.println(contractor.name() + " => " + contractor.fee().toPlainString());
        }
    }
}