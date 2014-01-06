/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kis.sqlparser.SqlAnalizer.*;
import kis.sqlparser.Table.Tuple;
import org.junit.Test;

/**
 *
 * @author naoki
 */
public class SqlAnalizerTest {
    
    public SqlAnalizerTest() {
    }

    @Test
    public void testSomeMethod() {
        Map<Column, Integer> cols = new HashMap<>();
        cols.put(new Column("test"), 0);
        cols.put(new Column("id"), 1);
        cols.put(new Column("name"), 2);
        Tuple collect = new Tuple(1, Stream.of(true, 123, "ほげ")
                .map(o -> Optional.of(o))
                .collect(Collectors.toList()));
        System.out.println(SqlAnalizer.eval(new IntValue(3), cols, collect));
        System.out.println(SqlAnalizer.eval(new BinaryOp(new IntValue(3), new IntValue(3), "="), cols, collect));
        System.out.println(SqlAnalizer.eval(new BinaryOp(new IntValue(2), new IntValue(3), "="), cols, collect));
        System.out.println(SqlAnalizer.eval(new BinaryOp(new IntValue(2), new IntValue(3), "<"), cols, collect));
        System.out.println(SqlAnalizer.eval(new BinaryOp(new IntValue(2), new IntValue(3), ">"), cols, collect));
        System.out.println(SqlAnalizer.eval(new BinaryOp(new BinaryOp(new IntValue(2), new IntValue(3), ">"), new BinaryOp(new IntValue(2), new IntValue(3), "<"),"or"), cols, collect));
        System.out.println(SqlAnalizer.eval(new BinaryOp(new BinaryOp(new IntValue(2), new IntValue(3), ">"), new BinaryOp(new IntValue(2), new IntValue(3), "<"),"and"), cols, collect));
        System.out.println(SqlAnalizer.eval(new FieldValue(new Column("id")), cols, collect));
        System.out.println(SqlAnalizer.eval(new BinaryOp(new FieldValue(new Column("id")), new IntValue(123), "="), cols, collect));
    }
    
}
