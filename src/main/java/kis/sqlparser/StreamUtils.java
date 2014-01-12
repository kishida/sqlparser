/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    public static <T, U> Stream<Pair<T,U>> zip(Stream<T> f, Stream<U> s){
        Iterator<T> fite = f.iterator();
        Iterator<U> site = s.iterator();
        Iterator<Pair<T, U>> iterator = new Iterator<Pair<T, U>>(){
            @Override
            public boolean hasNext() {
                return fite.hasNext() && site.hasNext();
            }
            @Override
            public Pair<T, U> next() {
                return Pair.of(fite.next(), site.next());
            }
        };
        
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        iterator, Spliterator.NONNULL | Spliterator.ORDERED), false);
    }
}
