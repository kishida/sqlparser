/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static kis.sqlparser.ObjectPatternMatch.*;
import kis.sqlparser.SqlParser.*;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.codehaus.jparsec.Parser;

/**
 *
 * @author naoki
 */
public class SqlAnalizer {
    public static interface SqlValue{
        
    }
    @AllArgsConstructor @ToString
    public static class StringValue implements SqlValue{
        String value;
    }
    @AllArgsConstructor @ToString
    public static class BooleanValue implements SqlValue{
        boolean value;
    }
    @AllArgsConstructor @ToString
    public static class IntValue implements SqlValue{
        int value;
    }
    public static class NullValue implements SqlValue{
    }
    public static class Wildcard implements SqlValue{
    }
    @AllArgsConstructor @ToString
    public static class BinaryOp implements SqlValue{
        SqlValue left;
        SqlValue right;
        String op;
    }
    
    @AllArgsConstructor @ToString
    public static class FieldValue implements SqlValue{
        Column column;
    }
    
    @AllArgsConstructor @ToString
    public static class TernaryOp implements SqlValue{
        SqlValue cond;
        SqlValue first;
        SqlValue sec;
        String op;
    }
    
    public static SqlValue wrap(Optional<?> o){
        if(!o.isPresent()){
            return new NullValue();
        }
        return matchRet(o.get(),
            caseOfRet(String.class, s -> new StringValue(s)),
            caseOfRet(Integer.class, i -> new IntValue(i)),
            caseOfRet(Boolean.class, b -> new BooleanValue(b)),
            noMatchRet(() -> {throw new RuntimeException(o.getClass() + " is not supported");})
        );
    }
    
    public static Optional<?> unwrap(SqlValue v){
        return matchRet(v,
            caseOfRet(StringValue.class, s -> Optional.of(s.value)),
            caseOfRet(IntValue.class, i -> Optional.of(i.value)),
            caseOfRet(BooleanValue.class, b -> Optional.of(b.value)),
            caseOfRet(NullValue.class, n -> Optional.empty()),
            noMatchRet(() -> {throw new RuntimeException(v.getClass() + " is not supported");})
        );
        
    }
    
    public static SqlValue eval(SqlValue value, Map<Column, Integer> colIndex, List<Optional<?>> row){
        return matchRet(value, 
            caseOfRet(StringValue.class, s -> s),
            caseOfRet(BooleanValue.class, b -> b),
            caseOfRet(IntValue.class, i -> i),
            caseOfRet(NullValue.class, n -> n),
            caseOfRet(FieldValue.class, f -> {
                int idx = colIndex.getOrDefault(f.column, -1);
                if(idx < 0) throw new RuntimeException(f.column + " not found.");
                return wrap(row.get(idx));
            }),
            caseOfRet(BinaryOp.class, bin -> {
                SqlValue left = eval(bin.left, colIndex, row);
                SqlValue right = eval(bin.right, colIndex, row);
                if(left instanceof NullValue || right instanceof NullValue){
                    return new NullValue();
                }
                String op = bin.op.trim();
                if("and".equals(op) || "or".equals(op)){
                    if(left instanceof BooleanValue && right instanceof BooleanValue){
                        switch(op){
                            case "and":
                                return new BooleanValue(((BooleanValue)left).value & ((BooleanValue)right).value);
                            case "or":
                                return new BooleanValue(((BooleanValue)left).value | ((BooleanValue)right).value);
                        }
                    }else{
                        throw new RuntimeException(op + " operator need boolean value");
                    }
                }
                if(left instanceof IntValue && right instanceof IntValue){
                    int ileft = ((IntValue)left).value;
                    int iright = ((IntValue)right).value;
                    boolean  ret;
                    switch(op){
                        case "=":
                            ret = ileft == iright;
                            break;
                        case "<":
                            ret = ileft < iright;
                            break;
                        case ">":
                            ret = ileft > iright;
                            break;
                        case "<=":
                            ret = ileft <= iright;
                            break;
                        case ">=":
                            ret = ileft >= iright;
                            break;
                        default:
                            throw new RuntimeException("[" +op + "] operator is not supported");
                    }
                    return new BooleanValue(ret);
                }else{
                    throw new RuntimeException(op + " operator need int value");
                }
            }),
            caseOfRet(TernaryOp.class, ter ->{
                if(!"between".equals(ter.op)){
                    throw new RuntimeException(ter.op + " is not supported");
                }
                SqlValue cond = eval(ter.cond, colIndex, row);
                SqlValue first = eval(ter.first, colIndex, row);
                SqlValue sec = eval(ter.sec, colIndex, row);
                if(cond instanceof IntValue && first instanceof IntValue && sec instanceof IntValue){
                    int icond = ((IntValue)cond).value;
                    int ifirst = ((IntValue)first).value;
                    int isec = ((IntValue)sec).value;
                    return new BooleanValue((ifirst <= icond && icond <= isec) || (isec <= icond && icond <= ifirst));
                }else{
                    throw new RuntimeException("between need int value");
                }
            }),
            noMatchRet(() -> {throw new RuntimeException(value.getClass() + " is not suppoerted");})
        );
    }
        
    public static List<Column> findField(Map<String, Table> env, String name){
        return env.values().stream()
                .flatMap(t -> t.columns.stream())
                .filter(c -> c.name.equals(name))
                .collect(Collectors.toList());
    }

    public static abstract class QueryPlan{
        abstract List<Column> getColumns();
        abstract Iterator<Optional<List<Optional<?>>>> iterator();
        
        Map<Column, Integer> getColumnIndex(){
            Map m = new HashMap<>();
            Counter c = new Counter();
            getColumns().stream().forEach(col -> m.put(col, c.getCount() - 1));
            return m;
        }
        
    }
    @AllArgsConstructor
    public static abstract class NodePlan extends QueryPlan{
        QueryPlan from;
    }
    
    static class Counter{
        int count;
        Counter(){
            count = 0;
        }
        int getCount(){
            return ++count;
        }
    }    
    public static class SelectPlan extends NodePlan{
        List<SqlValue> values;
        public SelectPlan(QueryPlan from, List<SqlValue> values){
            super(from);
            this.values = values;
        }
        @Override
        List<Column> getColumns() {
            Counter c = new Counter();
            return values.stream().flatMap(v -> 
                v instanceof Wildcard ? 
                    from.getColumns().stream() :
                    Stream.of((v instanceof FieldValue) ?
                        ((FieldValue)v).column : 
                        new Column(c.getCount() + "")))
                .collect(Collectors.toList());
        }

        @Override
        Iterator<Optional<List<Optional<?>>>> iterator() {
            Iterator<Optional<List<Optional<?>>>> ite = from.iterator();
            Map<Column, Integer> columnIndex = from.getColumnIndex();
            
            return new Iterator<Optional<List<Optional<?>>>>() {

                @Override
                public boolean hasNext() {
                    return ite.hasNext();
                }

                @Override
                public Optional<List<Optional<?>>> next() {
                    Optional<List<Optional<?>>> line = ite.next();
                    if(!line.isPresent()) return line;
                    
                    return Optional.of(values.stream().flatMap(c -> {
                        if(c instanceof Wildcard){
                            return from.getColumns().stream().map(FieldValue::new);
                        }else{
                            return Stream.of(c);
                        }
                    })
                        .map(c -> eval(c, columnIndex, line.get()))
                        .map(v -> unwrap(v))
                        .collect(Collectors.toList()));
                }
            };
        }

    }

    public static class FilterPlan extends NodePlan{
        SqlValue cond;

        public FilterPlan(QueryPlan from, SqlValue cond) {
            super(from);
            this.cond = cond;
        }
        @Override
        List<Column> getColumns() {
            return from.getColumns();
        }

        @Override
        Iterator<Optional<List<Optional<?>>>> iterator() {
            Iterator<Optional<List<Optional<?>>>> ite = from.iterator();
            Map<Column, Integer> colindex = from.getColumnIndex();
            
            return new Iterator<Optional<List<Optional<?>>>>(){
                @Override
                public boolean hasNext() {
                    return ite.hasNext();
                }

                @Override
                public Optional<List<Optional<?>>> next() {
                    Optional<List<Optional<?>>> optLine = ite.next();
                    if(!optLine.isPresent()) return optLine;
                    List<Optional<?>> line = optLine.get();
                    SqlValue val = eval(cond, colindex, line);
                    if(val instanceof BooleanValue && ((BooleanValue)val).value){
                        return optLine;
                    }else{
                        return Optional.empty();
                    }
                }
                
            };
        }
    }
    @AllArgsConstructor
    public static class TablePlan extends QueryPlan{
        Table table;

        @Override
        List<Column> getColumns() {
            return table.columns;
        }
        
        @Override
        Iterator<Optional<List<Optional<?>>>> iterator(){
            return table.data.stream().map(l -> Optional.of(l)).iterator();
        }
    }
    public static class JoinPlan extends NodePlan{
        QueryPlan secondary;
        SqlValue cond;

        public JoinPlan(QueryPlan from, QueryPlan secondary, SqlValue cond) {
            super(from);
            this.secondary = secondary;
            this.cond = cond;
        }
        @Override
        List<Column> getColumns() {
            return Stream.concat(
                    from.getColumns().stream(), 
                    secondary.getColumns().stream()).collect(Collectors.toList());
        }

        @Override
        Iterator<Optional<List<Optional<?>>>> iterator() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
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
                        .orElseThrow(() -> new RuntimeException(
                                "table " + f.table.ident + " not found"))
                        .columns.stream().filter(c -> c.name.equals(f.field.ident))
                        .findFirst().map(c -> new FieldValue(c))
                        .orElseThrow(() -> new RuntimeException(
                                "field " + f.field.ident + " of " + f.table.ident + " not found"))
            ),
            noMatchRet(() -> {
                    throw new RuntimeException(ast.getClass().getName() + " is wrong type");})
        );
    }
    
    public static SelectPlan analize(Schema sc, SqlParser.ASTSql sql){
        Map<String, Table> env = new HashMap<>();

        //From解析
        SqlParser.ASTFrom from = sql.from;
        
        Table table = sc.find(from.table.ident)
                .orElseThrow(() -> 
                        new RuntimeException("table " + from.table.ident + " not found"));
        env.put(from.table.ident, table);
        QueryPlan primary = new TablePlan(table);
        
        for(ASTJoin j : from.joins){
            String t = j.table.ident;
            Table tb = sc.find(t).orElseThrow(() ->
                new RuntimeException("join table " + t + " not found"));
            env.put(t, tb);
            SqlValue cond = validate(env, j.logic);
            TablePlan right = new TablePlan(tb);
            primary = new JoinPlan(primary, right, cond);
        }
        
        // where 解析
        Optional<SqlValue> cond = sql.where.map(a -> validate(env, a));
        
        if(cond.isPresent()){
            primary = new FilterPlan(primary, cond.get());
        }

        //Select解析
        SqlParser.ASTSelect sel = sql.select;
        List<SqlValue> columns = sel.cols.stream()
                .map(c -> validate(env, c))
                .collect(Collectors.toList());
        return new SelectPlan(primary, columns);
    }
    
    public static void main(String[] args) {
        Table tshohin = new Table("shohin", Stream.of("id", "name", "bunrui_id", "price")
                .map(s -> new Column(s)).collect(Collectors.toList()));
        Table tbunrui = new Table("bunrui", Stream.of("id", "name")
                .map(s -> new Column(s)).collect(Collectors.toList()));
        tbunrui
            .insert(1, "野菜")
            .insert(2, "くだもの")
            .insert(3, "菓子");
        tshohin
            .insert(1, "りんご", 2, 250)
            .insert(2, "キャベツ", 1, 200)
            .insert(3, "たけのこ", 3, 150);
        
        Schema sc = new Schema(Arrays.asList(tshohin, tbunrui));
        Parser<SqlParser.ASTSql> parser = SqlParser.parser();
        SqlParser.ASTSql sql = parser.parse("select id, name from shohin where price <= 200");
        SelectPlan plan = analize(sc, sql);
        System.out.println("shohin--");
        plan.iterator().forEachRemaining(line ->{
            line.ifPresent(l -> {
                System.out.println(l.stream().map(o -> o.isPresent() ? o.get().toString() : "null").collect(Collectors.joining(",", "[", "]")));
            });
        });
        
        SqlParser.ASTSql sql2 = parser.parse("select shohin.id, shohin.name,bunrui.name"
                + " from shohin left join bunrui on shohin.bunrui_id=bunrui.id");
        analize(sc, sql2);
    }
}
