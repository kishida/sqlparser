/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import kis.sqlparser.Table.Tuple;
import lombok.AllArgsConstructor;

/**
 *
 * @author naoki
 */
@AllArgsConstructor
public abstract class Index {
        
    public Map<Object, List<Tuple>> tuples;
    int targetColumn;
    
    public void insert(Tuple tuple){
        withKey(tuple).ifPresent(key -> 
            tuples.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple));
    }
    public void delete(Tuple tuple){
        withKey(tuple).ifPresent(key -> 
            tuples.computeIfPresent(key, (k, v) -> {
                v.remove(tuple);
                return v.isEmpty() ? null : v;
            }));
    }
    public void update(List<Optional<?>> old, Tuple newTuple){
        Optional<?> newKey = withKey(newTuple);
        if(old.size() > targetColumn || old.get(targetColumn).isPresent()){
            if(newKey.isPresent()){
                if(old.get(targetColumn).get().equals(newKey.get())){
                    return;
                }
            }
            tuples.computeIfPresent(old.get(targetColumn).get(), (k, v) -> {
                for(Iterator<Tuple> ite = v.iterator(); ite.hasNext();){
                    Tuple t = ite.next();
                    if(t.rid == newTuple.rid) ite.remove();
                }
                return v.isEmpty() ? null : v;
            });
        }
        insert(newTuple);
    }
    
    Optional<?> withKey(Tuple tuple){
        if(tuple.row.size() <= targetColumn || !tuple.row.get(targetColumn).isPresent()){
            return Optional.empty();
        }
        return tuple.row.get(targetColumn);
    }
    
    public Iterator<Tuple> equalsTo(Object value){
        return tuples.getOrDefault(value, Collections.EMPTY_LIST).iterator();
    }
    
    public static class HashIndex extends Index{
        public HashIndex(int columnIndex){
            super(new LinkedHashMap<>(), columnIndex);
        }
    }
    public static class TreeIndex extends Index{
        public TreeIndex(int targetColumn) {
            super(new TreeMap<>(), targetColumn);
        }
        public Iterator<Tuple> between(Object from, Object to){
            return flatten(((TreeMap<Object, List<Tuple>>)tuples).subMap(from, true, to, true));
        }
        public Iterator<Tuple> compare(Object v, String op){
            TreeMap<Object, List<Tuple>> tm = (TreeMap<Object, List<Tuple>>)tuples;
            SortedMap<?, List<Tuple>> m = 
                    "<".equals(op) ? tm.headMap(v, false) :
                    "<=".equals(op) ? tm.headMap(v, true) :
                    ">".equals(op) ? tm.tailMap(v, false) :
                    ">=".equals(op) ? tm.tailMap(v, true) : 
                    (SortedMap<?, List<Tuple>>)Collections.EMPTY_MAP;
            return flatten(m);
        }
        
        <T> Iterator<T> flatten(SortedMap<?, List<T>> m){
            return m.values().stream()
                    .flatMap(ts -> ts.stream()).iterator();
        }
    }
}
