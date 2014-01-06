/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 *
 * @author naoki
 */
public class StreamUtils {
    public static <T, U> void zip(Stream<T> f, Stream<U> s, BiConsumer<T, U> bc){
        Iterator<T> fite = f.iterator();
        Iterator<U> site = s.iterator();
        while(fite.hasNext() && site.hasNext()){
            bc.accept(fite.next(), site.next());
        }
    }
}
