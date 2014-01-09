/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.AllArgsConstructor;

/**
 *
 * @author naoki
 */
@AllArgsConstructor
public class Pair<T, U> {
    T left;
    U right;
    
    public static <M, N> Pair<M, N> of(M left, N right){
        return new Pair<>(left, right);
    }
    public void with(BiConsumer<T, U> cons){
        cons.accept(left, right);
    }
    public<R> R reduce(BiFunction<T, U, R> func){
        return func.apply(left, right);
    }
    public<M, N> Pair<M, N> map(Function<T, M> lfunc, Function<U, N> rfunc){
        return of(lfunc.apply(left), rfunc.apply(right));
    }
    public<M, N> Pair<M, N> flatMap(BiFunction<T, U, Pair<M, N>> func){
        return func.apply(left, right);
    }
}
