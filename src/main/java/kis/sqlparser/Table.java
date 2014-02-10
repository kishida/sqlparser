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
        Table table;
        long createTx;
        long commitTx;
        boolean modified;

        public TableTuple(Table table, long rid, Transaction tx, List<Optional<?>> row) {
            this(table, rid, tx.txId, row);
        }
        public TableTuple(Table table, long rid, long txid, List<Optional<?>> row) {
            super(rid, row);
            this.table = table;
            this.createTx = txid;
            commitTx = 0;
            modified = false;
        }
        public TableTuple(TableTuple tt){
            this(tt.table, tt.rid, tt.createTx, tt.row);
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
        TableTuple tuple = new TableTuple(this, rid, tx,
                Arrays.stream(values)
                        .map(Optional::ofNullable)
                        .collect(Collectors.toList()));
        tx.insertTuples.add(tuple);
        dataInsert(tuple);
        return this;
    }
    public void dataInsert(TableTuple tuple){
        data.put(tuple.rid, tuple);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.insert(tuple));
    }

    void update(Transaction tx, long rid, List<Optional<?>> copy) {
        //自分のトランザクションで削除したものは変更できないし、よそのトランザクションで削除・変更されたものも変更できない
        TableTuple oldtuple = data.get(rid);
        if(oldtuple == null || oldtuple.modified){
            throw new RuntimeException("modify conflict");
        }
        //元の値を保存
        TableTuple tuple = new TableTuple(oldtuple);
        tuple.commitTx = 0;//未コミット
        tuple.createTx = tx.txId;
        //変更反映
        tuple.row = copy;
        dataUpdate(tuple, oldtuple);
        if(oldtuple.createTx == tx.txId){
            //自分のトランザクションで変更したデータは履歴をとらない
            return;
        }
        oldtuple.modified = true;
        //履歴を保存
        ModifiedTuple.Updated ud = new ModifiedTuple.Updated(oldtuple, tuple, tx.txId);
        addModifiedTuple(ud);
        tx.modifiedTuples.add(ud);
    }
    public void dataUpdate(TableTuple tuple, TableTuple oldtuple){
        data.put(tuple.rid, tuple);
        indexes.values().stream().flatMap(is -> is.stream()).forEach(idx -> idx.update(oldtuple.row, tuple));
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
    
    List<TableTuple> getModifiedTuples(Optional<Transaction> otx){
        if(!otx.isPresent()){
            return modifiedTuples.values().stream()
                    .map(list -> list.stream()
                            .filter(mt -> (mt.isCommited() && mt instanceof ModifiedTuple.Updated) ||//コミットされてる更新
                                    (!mt.isCommited() && mt instanceof ModifiedTuple.Deleted)) //もしくはコミットされてない削除
                            .map(mt -> mt instanceof ModifiedTuple.Updated ? ((ModifiedTuple.Updated)mt).newTuple : mt.oldtuple)
                            .findFirst())
                    .filter(omt -> omt.isPresent()).map(omt -> omt.get())
                    .collect(Collectors.toList());
        }
        Transaction tx = otx.get();
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
