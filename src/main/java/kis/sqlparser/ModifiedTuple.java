/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import kis.sqlparser.Table.TableTuple;

/**
 *
 * @author naoki
 */
public class ModifiedTuple {
    TableTuple oldtuple;
    long modiryTx;
    long commitTx;

    public ModifiedTuple(TableTuple oldtuple, long modiryTx) {
        this.oldtuple = oldtuple;
        this.modiryTx = modiryTx;
        this.commitTx = 0;
    }
    
    public static class Deleted extends ModifiedTuple{
        public Deleted(TableTuple oldtuple, long modiryTx) {
            super(oldtuple, modiryTx);
        }
    }
    public static class Updated extends ModifiedTuple{
        TableTuple newTuple;
        public Updated(TableTuple oldtuple, TableTuple newTuple, long modiryTx) {
            super(oldtuple, modiryTx);
            this.newTuple = newTuple;
        }
    }
    public boolean isCommited(){
        return commitTx != 0;
    }
}
