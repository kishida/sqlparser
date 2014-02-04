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
import java.util.LinkedList;
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
        long createTx;
        long commitTx;
        boolean modified;

        public TableTuple(long rid, Transaction tx, List<Optional<?>> row) {
            this(rid, tx.txId, row);
        }
        public TableTuple(long rid, long txid, List<Optional<?>> row) {
            super(rid, row);
            this.createTx = txid;
            commitTx = 0;
            modified = false;
        }
        public TableTuple(TableTuple tt){
            this(tt.rid, tt.createTx, tt.row);
            commitTx = tt.commitTx;
            modified = tt.modified;
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
    Map<Column, List<Index>> indexes;
    
    LinkedHashMap<Long, TableTuple> data;
    HashMap<Long, LinkedList<ModifiedTuple>> modifiedTuples;
    
    public Table(String name, List<Column> columns){
        this.name = name;
        this.columns = columns.stream()
                .map(col -> new Column(this, col.name))
                .collect(Collectors.toList());
        this.data = new LinkedHashMap<>();
        this.modifiedTuples = new HashMap<>();
        this.indexes = new HashMap<>();
    }
    
    public Table insert(Transaction tx, Object... values){
        if(columns.size() < values.length){
            throw new RuntimeException("values count is over the number of columns");
        }
        ++rid;
        TableTuple tuple = new TableTuple(rid, tx,
                Arrays.stream(values)
                        .map(Optional::ofNullable)
                        .collect(Collectors.toList()));
        data.put(rid, tuple);
        tx.insertTuples.add(tuple);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.insert(tuple));
        return this;
    }

    void update(Transaction tx, long rid, List<Optional<?>> copy) {
        TableTuple tuple = data.get(rid);
        if(tuple.modified){
            throw new RuntimeException("modify conflict");
        }
        //元の値を保存
        TableTuple oldtuple = new TableTuple(tuple);
        oldtuple.modified = true;
        //変更反映
        tuple.row = copy;
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.update(oldtuple.row, tuple));
        if(tuple.createTx == tx.txId){
            //自分のトランザクションで変更したデータは履歴をとらない
            return;
        }
        tuple.commitTx = 0;//未コミット
        tuple.createTx = tx.txId;
        //履歴を保存
        ModifiedTuple.Updated ud = new ModifiedTuple.Updated(oldtuple, tuple, tx.txId);
        addModifiedTuple(ud);
        tx.modifiedTuples.add(ud);
    }

    void delete(Transaction tx, List<TableTuple> row) {
        if(row.stream().anyMatch(t -> t.modified)){
            throw new RuntimeException("modify conflict");
        }
        row.stream().map(t -> t.rid).forEach(data::remove);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> row.forEach(r -> idx.delete(r)));
        //履歴を保存
        row.stream().filter(t -> t.createTx != tx.txId).forEach(t -> {
            t.modified = true;
            ModifiedTuple.Deleted dt = new ModifiedTuple.Deleted(t, rid);
            addModifiedTuple(dt);
            tx.modifiedTuples.add(dt);
        });
                
    }
    void addIndex(Column left, Index idx) {
        indexes.computeIfAbsent(left, c -> new ArrayList<>()).add(idx);
    }
    
    List<TableTuple> getModifiedTuples(Transaction tx){
        return modifiedTuples.values().stream()
                .map(list -> list.stream()
                        .filter(mt -> mt.modiryTx != tx.txId)//自分のトランザクションで変更されたものは省く
                        .filter(mt -> !mt.isCommited() || (mt.commitTx >= tx.txId))
                        .map(mt -> mt.oldtuple)
                        .filter(ot -> tx.tupleAvailable(ot)).findFirst())
                .filter(omt -> omt.isPresent()).map(omt -> omt.get())
                .collect(Collectors.toList());
    }
    
    void addModifiedTuple(ModifiedTuple mt){
        modifiedTuples.computeIfAbsent(mt.oldtuple.rid, rid -> new LinkedList())
                .push(mt);
    }
    void removeModifiedTuple(ModifiedTuple mt){
        modifiedTuples.computeIfPresent(mt.oldtuple.rid, (rid, list) -> {
            list.remove(mt);
            return list.isEmpty() ? null : list;
        });
    }
}
