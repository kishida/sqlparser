/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import lombok.AllArgsConstructor;

/**
 *
 * @author naoki
 */
@AllArgsConstructor
public class Context {
    Schema schema;
    
    public void exec(String sql){
        SqlAnalizer.exec(this, sql);
    }
}
