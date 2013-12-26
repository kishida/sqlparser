/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.codehaus.jparsec.Parser;

/**
 *
 * @author naoki
 */
public class SqlAnalizer {
    public static class BinaryOp{
        Object left;
        Object right;
        String op;
    }
    
    public static void analize(Schema sc, SqlParser.ASTSql sql){
        Map<String, Table> env = new HashMap<>();

        //From解析
        SqlParser.ASTFrom from = sql.from;
        
        Table table = sc.find(from.table.ident)
                .orElseThrow(() -> 
                        new RuntimeException("table " + from.table.ident + " not found"));
        env.put(from.table.ident, table);
        
        from.joins.stream().forEach(j ->{
            String t = j.table.ident;
            Table tb = sc.find(t).orElseThrow(() ->
                new RuntimeException("join table " + t + " not found"));
            env.put(t, tb);
            
        });
        
        //Select解析
        SqlParser.ASTSelect sel = sql.select;
       
    }
    
    public static void main(String[] args) {
        Schema sc = new Schema(
            Arrays.asList(
                new Table("shohin", Stream.of("id", "name", "bunrui_id", "price")
                        .map(s -> new Column(s)).collect(Collectors.toList())),
                new Table("bunrui", Stream.of("id", "name")
                        .map(s -> new Column(s)).collect(Collectors.toList()))
            )
        );
        Parser<SqlParser.ASTSql> parser = SqlParser.parser();
        SqlParser.ASTSql sql = parser.parse("select shohin.id, shohin.name from shohin");
        analize(sc, sql);
        SqlParser.ASTSql sql2 = parser.parse("select shohin.id, shohin.name from shohin left join bunrui on shohin.bunrui_id=bunrui.id");
        analize(sc, sql2);
    }
}
