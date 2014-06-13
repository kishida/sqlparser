/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import kis.sqlparser.SqlAnalizer.Records;
import kis.sqlparser.Table.Tuple;

/**
 *
 * @author naoki
 */
public class Context {
    Schema schema;

    public Context(Schema schema) {
        this.schema = schema;
    }
    
    public Records<Tuple> exec(String sql){
        return SqlAnalizer.exec(this, sql);
    }

}
