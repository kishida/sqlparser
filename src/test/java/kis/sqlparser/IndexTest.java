/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import static java.util.stream.Collectors.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import kis.sqlparser.Table.Tuple;
import org.junit.Test;

/**
 *
 * @author naoki
 */
public class IndexTest {
    List<Tuple> ts;
    public IndexTest() {
        Object[][] dt = {
            {1, "test1"},
            {2, "test2"},
            {3, "test3"},
            {2, "test4"},
            {5, "test5"},
            {4, "test6"},
        };
        ts = new ArrayList<>();
        StreamUtils.zip(Arrays.stream(dt), IntStream.iterate(0, i -> i + 1).boxed(), (d, i) -> {
            ts.add(new Table.Tuple(i, Stream.of(d[0], d[1]).map(Optional::of).collect(toList())));
        });
    }

    @Test
    public void TreeIndexのinsert() {
        Index idx = new Index.TreeIndex(0);
        ts.forEach(t -> idx.insert(t));
        print(idx);
    }
    @Test
    public void HashIndexのinsert() {
        Index idx = new Index.HashIndex(0);
        ts.forEach(t -> idx.insert(t));
        print(idx);
    }

    @Test
    public void 該当値が全部きえるdelete(){
        Index idx = new Index.HashIndex(0);
        ts.forEach(t -> idx.insert(t));
        idx.delete(ts.get(2));
        print(idx);
        System.out.println(idx.tuples.size());
    }
    @Test
    public void 該当値が一部きえるdelete(){
        Index idx = new Index.HashIndex(0);
        ts.forEach(t -> idx.insert(t));
        idx.delete(ts.get(3));
        print(idx);
        System.out.println(idx.tuples.size());
    }
    @Test
    public void キーが変更されるupdate(){
        Index idx = new Index.HashIndex(0);
        ts.forEach(t -> idx.insert(t));
        List<Optional<?>> old = new ArrayList<>(ts.get(0).row);
        ts.get(0).row.set(0, Optional.of(5));
        idx.update(old, ts.get(0));
        print(idx);
        System.out.println(idx.tuples.size());
    }
    @Test
    public void キーが変更されないupdate(){
        Index idx = new Index.HashIndex(0);
        ts.forEach(t -> idx.insert(t));
        List<Optional<?>> old = new ArrayList<>(ts.get(0).row);
        ts.get(0).row.set(1, Optional.of("TEST1"));
        idx.update(old, ts.get(0));
        print(idx);
    }
    @Test
    public void 範囲(){
        Index.TreeIndex idx = new Index.TreeIndex(0);
        ts.forEach(t -> idx.insert(t));
        idx.between(2, 4).forEachRemaining(this::print);
    }
    @Test
    public void 大なりイコール(){
        Index.TreeIndex idx = new Index.TreeIndex(0);
        ts.forEach(t -> idx.insert(t));
        idx.compare(2, ">=").forEachRemaining(this::print);
    }
    @Test
    public void 大なり(){
        Index.TreeIndex idx = new Index.TreeIndex(0);
        ts.forEach(t -> idx.insert(t));
        idx.compare(2, ">").forEachRemaining(this::print);
    }
    @Test
    public void 小なり(){
        Index.TreeIndex idx = new Index.TreeIndex(0);
        ts.forEach(t -> idx.insert(t));
        idx.compare(4, "<").forEachRemaining(this::print);
    }
    @Test
    public void 小なりイコール(){
        Index.TreeIndex idx = new Index.TreeIndex(0);
        ts.forEach(t -> idx.insert(t));
        idx.compare(4, "<=").forEachRemaining(this::print);
    }
    
    void print(Index idx){
        idx.tuples.forEach((k, v) -> {
            v.forEach(this::print);
        });
        
    }
    void print(Tuple r){
        System.out.printf("%d %s%n", r.rid, 
                r.row.stream().map(o -> o.map(s -> s.toString()).orElse("null")).collect(joining(",")));
    }
}
