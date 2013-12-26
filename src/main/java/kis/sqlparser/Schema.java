/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;

/**
 *
 * @author naoki
 */
@NoArgsConstructor
public class Schema {
    Map<String, Table> tables;
    
    public Schema(List<Table> tables){
        this.tables = tables.stream()
                .collect(Collectors.toMap(t -> t.name, t -> t));
    }
    
    public Optional<Table> find(String name){
        return Optional.ofNullable(tables.get(name));
    }
}
