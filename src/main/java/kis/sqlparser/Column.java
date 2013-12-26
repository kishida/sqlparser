/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Optional;
import lombok.AllArgsConstructor;

/**
 *
 * @author naoki
 */
@AllArgsConstructor
public class Column {
    Optional<Table> parent;
    String name;
    
    public Column(Table parent, String name){
        this(Optional.ofNullable(parent), name);
    }
    
    public Column(String name){
        this(Optional.empty(), name);
    }
}
