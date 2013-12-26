/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

/**
 *
 * @author naoki
 */
public class ObjectPatternMatch {
    public interface MatchCase{
        boolean match(Object o);
    }
    
    @AllArgsConstructor
    public static class CaseOf<T> implements MatchCase{
        Class<T> cls;
        Consumer<T> proc;
        
        @Override
        public boolean match(Object o){
            if(!cls.isInstance(o)){
                return false;
            }
            proc.accept((T)o);
            return true;
        }
    }
    
    public static void match(Object o, MatchCase... cos){
        for(MatchCase co : cos){
            if(co.match(o)) return;
        }
        //throw new RuntimeException("no match");
    }
    public static <T> CaseOf<T> caseOf(Class<T> cls, Consumer<T> proc){
        return new CaseOf<>(cls, proc);
    }
    public static MatchCase noMatch(Runnable proc){
        return o -> {proc.run(); return true;};
    }
    
    
    public interface MatchCaseRet<R>{
        boolean match(Object o);
        R proc(Object o);
    }
    @AllArgsConstructor
    public static class CaseOfRet<T, R> implements MatchCaseRet<R>{
        Class<T> cls;
        Function<T, R> func;
        @Override
        public boolean match(Object o) {
            return cls.isInstance(o);
        }

        @Override
        public R proc(Object o) {
            return func.apply((T)o);
        }
    }
    
    public static <R> R matchRet(Object o, MatchCaseRet<R>... cors){
        for(MatchCaseRet<R> cor : cors){
            if(cor.match(o)) return cor.proc(o);
        }
        return null;
    }
    
    public static <T, R> CaseOfRet<T, R> caseOfRet(Class<T> cls, Function<T, R> func){
        return new CaseOfRet<>(cls, func);
    }
    public static<R> MatchCaseRet<R> noMatchRet(Supplier<R> sup){
        return new MatchCaseRet<R>() {

            @Override
            public boolean match(Object o) {
                return true;
            }

            @Override
            public R proc(Object o) {
                return sup.get();
            }
        };
    }
        
}
