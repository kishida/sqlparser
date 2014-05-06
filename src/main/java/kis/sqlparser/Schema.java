/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 * @author naoki
 */
public class Schema {
    Map<String, Table> tables;
    long txId;
    LinkedList<Transaction> trans;

    public Schema() {
        this(new ArrayList<>());
    }
    
    public Schema(List<Table> tables){
        txId = 0;
        this.tables = tables.stream()
                .collect(Collectors.toMap(t -> t.name, t -> t));
        this.trans = new LinkedList<>();
    }
    
    public Optional<Table> find(String name){
        return Optional.ofNullable(tables.get(name));
    }
    
    public Context createContext(){
        return new Context(this);
    }
    public Transaction createTransaction(){
        ++txId;
        Transaction tx = new Transaction(this, txId);
        trans.add(tx);
        return tx;
    }
    
    public void removeTx(Transaction tx){
        trans.remove(tx);
    }
}
