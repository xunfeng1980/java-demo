package com.coralmesh.reactor;


import org.junit.Test;
import reactor.core.publisher.Flux;

public class FluxTest {

    Integer add(Integer a, Integer b) {
        return a + b;
    }

    Integer inc(Integer a) {
        return a + 1;
    }

    @Test
    public void test() {
        Flux<Integer> flux = Flux.range(1, 4);
        flux.subscribe(System.out::println);
        flux.subscribe(System.out::println);
        flux.map(s -> s - 1).subscribe(System.out::println);
        flux.flatMap(s -> Flux.just(s - 1)).subscribe(System.out::println);
        Flux<String> inFlux = Flux.just("baeldung", ".", "com");
        inFlux.flatMap(s -> Flux.just(s.toUpperCase().split(""))).subscribe(System.out::println);
        inFlux.map(s -> s.toUpperCase().split("")).subscribe(System.out::println);
    }
}
