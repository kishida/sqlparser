/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Optional;
import java.util.function.Consumer;
import kis.sqlparser.SqlAnalizer.Records;
import kis.sqlparser.Table.Tuple;

/**
 *
 * @author naoki
 */
public class Context {
    Schema schema;
    Optional<Transaction> currentTx;

    public Context(Schema schema) {
        this.schema = schema;
        currentTx = Optional.empty();
    }
    
    public Records<Tuple> exec(String sql){
        return SqlAnalizer.exec(this, sql);
    }

    public void begin(){
        if(currentTx.isPresent()){
            throw new RuntimeException("transaction already exist.");
        }
        currentTx = Optional.of(schema.createTransaction());
    }
    public void commit(){
        currentTx.orElseThrow(() -> new RuntimeException("transaciton does not begin"))
                .commit();
        end();
    }
    public void abort(){
        currentTx.orElseThrow(() -> new RuntimeException("transaciton does not begin"))
                .abort();
        end();
    }
    void end(){
        currentTx = Optional.empty();
    }
    
    public void withTx(Consumer<Transaction> cons){
        Transaction tx = currentTx.orElseGet(() -> schema.createTransaction());
        if(!tx.enable){
            throw new RuntimeException("transaction is not enabled.");
        }
        cons.accept(tx);
        
        if(!currentTx.isPresent()){
            tx.commit();
        }
    }
}
