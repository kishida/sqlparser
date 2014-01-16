/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

/**
 *
 * @author naoki
 */
public class Table {
    @AllArgsConstructor
    @EqualsAndHashCode(of = "rid")
    public static class Tuple{
        long rid;
        List<Optional<?>> row;
    }
    String name;
    List<Column> columns;
    static long rid;
    Map<Column, List<Index>> indexes;
    
    LinkedHashMap<Long, Tuple> data;
    
    public Table(String name, List<Column> columns){
        this.name = name;
        this.columns = columns.stream()
                .map(col -> new Column(this, col.name))
                .collect(Collectors.toList());
        this.data = new LinkedHashMap<>();
        this.indexes = new HashMap<>();
    }
    
    public Table insert(Object... values){
        if(columns.size() < values.length){
            throw new RuntimeException("values count is over the number of columns");
        }
        ++rid;
        Tuple tuple = new Tuple(rid,
                Arrays.stream(values)
                        .map(Optional::ofNullable)
                        .collect(Collectors.toList()));
        data.put(rid, tuple);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.insert(tuple));
        return this;
    }

    void update(long rid, List<Optional<?>> copy) {
        Tuple tuple = data.get(rid);
        List<Optional<?>> old = tuple.row;
        tuple.row = copy;
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.update(old, tuple));
    }

    void delete(List<Tuple> row) {
        row.stream().map(t -> t.rid).forEach(data::remove);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> row.forEach(r -> idx.delete(r)));
    }
    void addIndex(Column left, Index idx) {
        indexes.computeIfAbsent(left, c -> new ArrayList<>()).add(idx);
    }
}
