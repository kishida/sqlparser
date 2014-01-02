/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/**
 *
 * @author naoki
 */
public class Table {
    @AllArgsConstructor
    public static class Tuple{
        List<Optional<?>> row;
    }
    String name;
    List<Column> columns;
    
    List<Tuple> data;
    
    public Table(String name, List<Column> columns){
        this.name = name;
        this.columns = columns.stream()
                .map(col -> new Column(this, col.name))
                .collect(Collectors.toList());
        this.data = new ArrayList<>();
    }
    
    public Table insert(Object... values){
        if(columns.size() < values.length){
            throw new RuntimeException("values count is over the number of columns");
        }
        data.add(new Tuple(Arrays.stream(values)
                .map(Optional::ofNullable)
                .collect(Collectors.toList())));
        return this;
    }
}
