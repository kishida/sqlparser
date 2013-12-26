/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static kis.sqlparser.ObjectPatternMatch.*;
import kis.sqlparser.SqlParser.*;
import lombok.AllArgsConstructor;
import org.codehaus.jparsec.Parser;

/**
 *
 * @author naoki
 */
public class SqlAnalizer {
    public static interface SqlValue{
        
    }
    @AllArgsConstructor
    public static class StringValue implements SqlValue{
        String value;
    }
    @AllArgsConstructor
    public static class BooleanValue implements SqlValue{
        boolean value;
    }
    @AllArgsConstructor
    public static class IntValue implements SqlValue{
        int value;
    }
    public static class Wildcard implements SqlValue{
    }
    @AllArgsConstructor
    public static class BinaryOp implements SqlValue{
        SqlValue left;
        SqlValue right;
        String op;
    }
    
    @AllArgsConstructor
    public static class FieldValue implements SqlValue{
        Column column;
    }
    
    @AllArgsConstructor
    public static class TernaryOp implements SqlValue{
        SqlValue cond;
        SqlValue first;
        SqlValue sec;
        String op;
    }
    
    public static List<Column> findField(Map<String, Table> env, String name){
        return env.values().stream()
                .flatMap(t -> t.columns.stream())
                .filter(c -> c.name.equals(name))
                .collect(Collectors.toList());
    }

    public static SqlValue validate(Map<String, Table> env, AST ast){
        return matchRet(ast, 
            caseOfRet(ASTStr.class, s -> 
                    new StringValue(s.str)),
            caseOfRet(ASTLogic.class, l -> 
                    new BinaryOp(validate(env, l.left), validate(env, l.right), l.op)),
            caseOfRet(ASTCond.class, c -> 
                    new BinaryOp(validate(env, c.left), validate(env, c.right), c.op)),
            caseOfRet(ASTBetween.class, b -> 
                    new TernaryOp(validate(env, b.obj),validate(env, b.start), validate(env, b.end),
                            "between")),
            caseOfRet(ASTInt.class, i -> 
                    new IntValue(i.value)),
            caseOfRet(ASTIdent.class, id ->{
                List<Column> column = findField(env, id.ident);
                if(column.isEmpty()){
                    throw new RuntimeException(id.ident + " is not found");
                }else if(column.size() > 1){
                    throw new RuntimeException(id.ident + " is ambiguous");
                }else{
                    return new FieldValue(column.get(0));
                }
            }),
            caseOfRet(ASTWildcard.class, w ->
                new Wildcard()),
            caseOfRet(ASTFqn.class, f ->
                (SqlValue)Optional.ofNullable(env.get(f.table.ident))
                        .orElseThrow(() -> new RuntimeException("table " + f.table.ident + " not found"))
                        .columns.stream().filter(c -> c.name.equals(f.field.ident))
                        .findFirst().map(c -> new FieldValue(c))
                        .orElseThrow(() -> new RuntimeException("field " + f.field.ident + " of " + f.table.ident + " not found"))
            ),
            noMatchRet(() -> {
                    throw new RuntimeException(ast.getClass().getName() + " is wrong type");})
        );
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
            validate(env, j.logic);
        });
        
        //Select解析
        SqlParser.ASTSelect sel = sql.select;
        List<SqlValue> columns = sel.cols.stream()
                .map(c -> validate(env, c))
                .collect(Collectors.toList());
        // where 解析
        Optional<SqlValue> cond = sql.where.map(a -> validate(env, a));
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
        SqlParser.ASTSql sql2 = parser.parse("select shohin.id, shohin.name,bunrui.name from shohin left join bunrui on shohin.bunrui_id=bunrui.id");
        analize(sc, sql2);
    }
}
