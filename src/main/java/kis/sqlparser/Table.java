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
    
    public static class TableTuple extends Tuple{
        Table table;
        long createTx;
        long commitTx = 0;

        public TableTuple(long rid, List<Optional<?>> row) {
            super(rid, row);
        }

        public void commit(long txId){
            commitTx = txId;
        }
        public boolean isCommited(){
            return commitTx != 0;
        }
    }
    String name;
    List<Column> columns;
    static long rid;
    Map<Column, List<Index>> indexes = new HashMap<>();;
    
    LinkedHashMap<Long, TableTuple> data = new LinkedHashMap<>();;
    
    public Table(String name, List<Column> columns){
        this.name = name;
        this.columns = columns.stream()
                .map(col -> new Column(this, col.name))
                .collect(Collectors.toList());
    }
    
    public Table insert(Object... values){
        if(columns.size() < values.length){
            throw new RuntimeException("values count is over the number of columns");
        }
        ++rid;
        TableTuple tuple = new TableTuple( rid,
                Arrays.stream(values)
                        .map(Optional::ofNullable)
                        .collect(Collectors.toList()));
        dataInsert(tuple);
        return this;
    }
    public void dataInsert(TableTuple tuple){
        data.put(tuple.rid, tuple);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.insert(tuple));
    }

    void update(long rid, List<Optional<?>> copy) {
        TableTuple tuple = data.get(rid);
        List<Optional<?>> old = tuple.row;
        tuple.row = copy;
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.update(old, tuple));
    }

    public void dataUpdate(TableTuple tuple, TableTuple oldtuple){
        data.put(tuple.rid, tuple);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.update(oldtuple.row, tuple));
    }

    void delete(List<TableTuple> row) {

        row.stream().map(t -> t.rid).forEach(data::remove);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> row.forEach(r -> idx.delete(r)));
    }
    void addIndex(Column left, Index idx) {
        indexes.computeIfAbsent(left, c -> new ArrayList<>()).add(idx);
    }
    


}
