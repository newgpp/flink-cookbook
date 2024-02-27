package com.felix;

import org.apache.commons.codec.digest.MurmurHash2;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.stream.Stream;

public class BitMapTest {

    @Test
    public void murmur_hash_should_success(){

        Stream.iterate(1, x -> x + 1).limit(10)
              .forEach(x -> System.out.println(MurmurHash2.hash64(String.valueOf(x))));

    }

    @Test
    public void bitMapTest() {
        //given
        Roaring64NavigableMap map = new Roaring64NavigableMap();
        //when
        Stream.iterate(1, x -> x + 1).limit(1000000)
                .forEach(x -> map.add(MurmurHash2.hash64(String.valueOf(x))));
        //then
        System.out.println(map.getLongCardinality());

    }
}
