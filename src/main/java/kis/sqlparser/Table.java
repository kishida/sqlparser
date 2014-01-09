/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
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
    
    LinkedHashMap<Long, Tuple> data;
    
    public Table(String name, List<Column> columns){
        this.name = name;
        this.columns = columns.stream()
                .map(col -> new Column(this, col.name))
                .collect(Collectors.toList());
        this.data = new LinkedHashMap<>();
    }
    
    public Table insert(Object... values){
        if(columns.size() < values.length){
            throw new RuntimeException("values count is over the number of columns");
        }
        ++rid;
        data.put(rid, new Tuple(rid,
                Arrays.stream(values)
                    .map(Optional::ofNullable)
                    .collect(Collectors.toList())));
        return this;
    }

    void update(long rid, List<Optional<?>> copy) {
        data.get(rid).row = copy;
    }

    void delete(List<Tuple> row) {
        row.stream().map(t -> t.rid).forEach(data::remove);
    }
}
