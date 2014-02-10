/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.ArrayList;
import java.util.List;
import kis.sqlparser.Table.TableTuple;
import lombok.EqualsAndHashCode;

/**
 *
 * @author naoki
 */
@EqualsAndHashCode(of = {"schema", "txId"})
public class Transaction {
    Schema schema;
    long txId;
    boolean enable;
    List<Table.TableTuple> insertTuples;
    List<ModifiedTuple> modifiedTuples;

    public Transaction(Schema schema, long txId) {
        this.schema = schema;
        this.txId = txId;
        enable = true;
        insertTuples = new ArrayList<>();
        modifiedTuples = new ArrayList<>();
    }
    
    public void commit(){
        end();
        insertTuples.forEach(t -> t.commit(schema.txId));
        
        schema.removeFinTx();
    }
    
    public void abort(){
        end();
        modifiedTuples.forEach(mt -> mt.abort());
        schema.removeTx(this);
    }
    
    private void end(){
        if(!enable){
            throw new RuntimeException("transaction is not enabled");
        }
        enable = false;
    }
    
    public void removeModified(){
        modifiedTuples.forEach(mt -> mt.oldtuple.table.removeModifiedTuple(mt));
    }
    
    public boolean tupleAvailable(TableTuple tuple){
        if(tuple.createTx != txId){
            //他のトランザクションのデータ
            if(tuple.isCommited()){
                //あとのトランザクションでコミットしたものは飛ばす
                if(tuple.commitTx >= txId) return false;
            }else{
                //コミットされていないものは飛ばす
                return false;
            }
        }
        return true;
    }
}
