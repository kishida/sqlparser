/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author naoki
 */
public class StreamUtilsTest {
    
    public StreamUtilsTest() {
    }

    @Test
    public void testSomeMethod() {
        StreamUtils.zip(
                Stream.of("abc", "cde", "def"),
                IntStream.iterate(1, i -> i + 1).boxed(), 
                (s, i) -> System.out.printf("%d:%s%n", i, s));
    }
    @Test
    public void testZipStream(){
        StreamUtils.zip(
                Stream.of("abc", "cde", "def"),
                Stream.iterate(1, i -> i + 1))
                .map(p -> p.reduce((s, i) -> String.format("%d:%s", i, s)))
                .forEach(s -> System.out.println(s));
    }
}
