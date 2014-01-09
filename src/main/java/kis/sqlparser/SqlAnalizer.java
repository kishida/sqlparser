/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static kis.sqlparser.ObjectPatternMatch.*;
import kis.sqlparser.SqlParser.*;
import kis.sqlparser.Table.Tuple;
import lombok.AllArgsConstructor;
import org.codehaus.jparsec.Parser;

/**
 *
 * @author naoki
 */
public class SqlAnalizer {
    public static interface SqlValue{// extends ASTExp{
        
    }
    @AllArgsConstructor
    public static class BooleanValue implements SqlValue{
        boolean value;
        
        @Override
        public String toString() {
            return value + "";
        }
    }
    public static class NullValue implements SqlValue{
        @Override
        public String toString() {
            return "NULL";
        }
    }
    
    @AllArgsConstructor
    public static class BinaryOp implements SqlValue{
        SqlValue left;
        SqlValue right;
        String op;

        @Override
        public String toString() {
            return String.format("%s %s %s", left, op, right);
        }
    }
    
    @AllArgsConstructor
    public static class FieldValue implements SqlValue{
        Column column;

        @Override
        public String toString() {
            return column.parent.map(t -> t.name + ".").orElse("") + column.name;
        }
    }
    
    @AllArgsConstructor
    public static class TernaryOp implements SqlValue{
        SqlValue cond;
        SqlValue first;
        SqlValue sec;
        String op;
        
        @Override
        public String toString() {
            return String.format("%s %s:%s:%s", op, cond, first, sec);
        }
    }
    
    public static SqlValue wrap(Optional<?> o){
        if(!o.isPresent()){
            return new NullValue();
        }
        return matchRet(o.get(),
            caseOfRet(String.class, s -> new StringValue(s)),
            caseOfRet(Integer.class, i -> new IntValue(i)),
            caseOfRet(Boolean.class, b -> new BooleanValue(b)),
            noMatchThrow(() -> new RuntimeException(o.getClass() + " is not supported"))
        );
    }
    
    public static Optional<?> unwrap(SqlValue v){
        return matchRet(v,
            caseOfRet(StringValue.class, s -> Optional.of(s.value)),
            caseOfRet(IntValue.class, i -> Optional.of(i.value)),
            caseOfRet(BooleanValue.class, b -> Optional.of(b.value)),
            caseOfRet(NullValue.class, n -> Optional.empty()),
            noMatchThrow(() -> new RuntimeException(v.getClass() + " is not supported"))
        );
        
    }
    
    public static SqlValue eval(SqlValue value, Map<Column, Integer> colIndex, Tuple tuple){
        return matchRet(value, 
            caseOfRet(StringValue.class, s -> s),
            caseOfRet(BooleanValue.class, b -> b),
            caseOfRet(IntValue.class, i -> i),
            caseOfRet(NullValue.class, n -> n),
            caseOfRet(FieldValue.class, f -> {
                int idx = colIndex.getOrDefault(f.column, -1);
                if(idx < 0) throw new RuntimeException(f.column + " not found.");
                if(idx >= tuple.row.size()) return new NullValue();
                return wrap(tuple.row.get(idx));
            }),
            caseOfRet(BinaryOp.class, bin -> {
                SqlValue left = eval(bin.left, colIndex, tuple);
                SqlValue right = eval(bin.right, colIndex, tuple);
                if(left instanceof NullValue || right instanceof NullValue){
                    return new NullValue();
                }
                String op = bin.op;
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
                        default:{
                            int iret;
                            switch(op){
                                case "+":
                                    iret = ileft + iright;
                                    break;
                                case "-":
                                    iret = ileft - iright;
                                    break;
                                case "*":
                                    iret = ileft * iright;
                                    break;
                                case "/":
                                    iret = ileft / iright;
                                    break;
                            default:
                                throw new RuntimeException("[" +op + "] operator is not supported");
                            }
                            return new IntValue(iret);
                        }
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
                SqlValue cond = eval(ter.cond, colIndex, tuple);
                SqlValue first = eval(ter.first, colIndex, tuple);
                SqlValue sec = eval(ter.sec, colIndex, tuple);
                if(cond instanceof NullValue || first instanceof NullValue || sec instanceof NullValue){
                    return new NullValue();
                }else if(cond instanceof IntValue && first instanceof IntValue && sec instanceof IntValue){
                    int icond = ((IntValue)cond).value;
                    int ifirst = ((IntValue)first).value;
                    int isec = ((IntValue)sec).value;
                    return new BooleanValue((ifirst <= icond && icond <= isec) || (isec <= icond && icond <= ifirst));
                }else{
                    throw new RuntimeException("between operator need int value");
                }
            }),
            noMatchThrow(() -> new RuntimeException(value.getClass() + " is not suppoerted"))
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
        abstract Records<Tuple> records();
        Optional<NodePlan> to;

        public QueryPlan() {
            this.to = Optional.empty();
        }

        public void setTo(NodePlan to) {
            this.to = Optional.ofNullable(to);
        }
        
        Map<Column, Integer> getColumnIndex(){
            Counter c = new Counter();
            return getColumns().stream().collect(Collectors.toMap(col -> col, col -> c.getCount() - 1));
        }
        
    }
    //@AllArgsConstructor
    public static abstract class NodePlan extends QueryPlan{
        QueryPlan from;
        public NodePlan(QueryPlan from) {
            this.from = from;
            from.setTo(this);
        }
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
    
    @FunctionalInterface
    static interface Records<T>{
        Optional<T> next();
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
                v instanceof ASTWildcard ? 
                    from.getColumns().stream() :
                    Stream.of((v instanceof FieldValue) ?
                        ((FieldValue)v).column : 
                        new Column(c.getCount() + "")))
                .collect(Collectors.toList());
        }

        @Override
        Records<Tuple> records() {
            Records<Tuple> records = from.records();
            Map<Column, Integer> columnIndex = from.getColumnIndex();
            
            return () -> {
                Optional<Tuple> line = records.next();
                if(!line.isPresent()) return line;

                List<Optional<?>> row = values.stream().flatMap(c -> {
                    if(c instanceof ASTWildcard){
                        return from.getColumns().stream().map(FieldValue::new);
                    }else{
                        return Stream.of(c);
                    }
                })
                        .map(c -> eval(c, columnIndex, line.get()))
                        .map(v -> unwrap(v))
                        .collect(Collectors.toList());
                return Optional.of(new Tuple(line.get().rid, row));
            };        
        }

        @Override
        public String toString() {
            return "select\n  <- " + from.toString();
        }
        
    }

    public static class FilterPlan extends NodePlan{
        List<SqlValue> conds;

        public FilterPlan(QueryPlan from, List<SqlValue> conds) {
            super(from);
            this.conds = conds;
        }
        public FilterPlan(QueryPlan from, SqlValue cond) {
            this(from, Arrays.asList(cond));
        }
     
        @Override
        List<Column> getColumns() {
            return from.getColumns();
        }

        @Override
        Records<Tuple> records() {
            Records<Tuple> records = from.records();
            Map<Column, Integer> columnIndex = from.getColumnIndex();
            
            return () -> {
                for(Optional<Tuple> optLine; (optLine = records.next()).isPresent();){
                    Tuple line = optLine.get();
                    boolean allTrue = conds.stream()
                            .map(cond -> eval(cond, columnIndex, line))
                            .allMatch(val -> val instanceof BooleanValue && ((BooleanValue)val).value);
                    if(allTrue){
                        return optLine;
                    }
                }
                return Optional.empty();
            };
        }

        @Override
        public String toString() {
            return "filter" + conds + "\n  <- " + from.toString();
        }
        
    }
    
    public static class EmptyPlan extends NodePlan{

        public EmptyPlan(QueryPlan from) {
            super(from);
        }

        @Override
        List<Column> getColumns() {
            return from.getColumns();
        }

        @Override
        Records<Tuple> records() {
            return () -> Optional.empty();
        }

        @Override
        public String toString() {
            return "empty\n  <- " + from.toString();
        }
    }
    
    public static class AdjustPlan extends NodePlan{

        public AdjustPlan(QueryPlan from) {
            super(from);
        }
        
        @Override
        List<Column> getColumns() {
            return from.getColumns();
        }

        @Override
        Records<Tuple> records() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    }
    
    public static class TablePlan extends QueryPlan{
        Table table;

        public TablePlan(Table table) {
            this.table = table;
        }

        @Override
        List<Column> getColumns() {
            return table.columns;
        }

        @Override
        Records<Tuple> records() {
            Iterator<Tuple> ite = table.data.values().iterator();
            return () -> {
                if(!ite.hasNext()) return Optional.empty();
                Tuple tuple = ite.next();
                return Optional.of(tuple);
            };
        }

        @Override
        public String toString() {
            return "table[" + table.name + "]";
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
        Records<Tuple> records() {
            Records<Tuple> records = from.records();
            Map<Column, Integer> columnIndex = getColumnIndex();
            return () -> {
                Optional<Tuple> optLine = records.next();
                if(!optLine.isPresent()) return optLine;
                Tuple line = optLine.get();
                Records<Tuple> srec = secondary.records();
                for(Optional<Tuple> sline;(sline = srec.next()).isPresent();){
                    Tuple joinline = new Tuple(0, Stream.concat(
                            line.row.stream(), sline.get().row.stream())
                            .collect(Collectors.toList()));
                    SqlValue v = eval(cond, columnIndex, joinline);
                    if(v instanceof BooleanValue && ((BooleanValue)v).value){
                        return Optional.of(joinline);
                    }
                }
                return Optional.of(new Tuple(0, Stream.concat(
                        Stream.concat(
                                line.row.stream(),
                                IntStream.range(0, getColumns().size() - line.row.size()).mapToObj(i -> Optional.empty())),
                        IntStream.range(0, secondary.getColumns().size()).mapToObj(i -> Optional.empty()))
                        .collect(Collectors.toList())));
            };
        }
        
        @Override
        public String toString() {
            return "join(nested loop)\n  <- " + from.toString() + "\n  /\n  <- " + secondary.toString();
        }

    }
    
    public static SqlValue validate(Map<String, Table> env, AST ast){
        return matchRet(ast, 
            caseOfRet(StringValue.class, s -> s),
            caseOfRet(IntValue.class, i -> i),
            caseOfRet(ASTWildcard.class, w -> w),
            caseOfRet(ASTBinaryOp.class, c -> 
                    new BinaryOp(validate(env, c.left), validate(env, c.right), c.op.trim())),
            caseOfRet(ASTTernaryOp.class, b -> 
                    new TernaryOp(validate(env, b.obj),validate(env, b.start), validate(env, b.end),
                            "between")),
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
            caseOfRet(ASTFqn.class, f ->
                (SqlValue)Optional.ofNullable(env.get(f.table.ident))
                        .orElseThrow(() -> new RuntimeException(
                                "table " + f.table.ident + " not found"))
                        .columns.stream().filter(c -> c.name.equals(f.field.ident))
                        .findFirst().map(c -> new FieldValue(c))
                        .orElseThrow(() -> new RuntimeException(
                                "field " + f.field.ident + " of " + f.table.ident + " not found"))
            ),
            noMatchThrow(() -> 
                    new RuntimeException(ast.getClass().getName() + " is wrong type"))
        );
    }
    
    public static SelectPlan analize(Schema sc, SqlParser.ASTSelect sql){
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
        List<SqlValue> columns = sql.select.stream()
                .map(c -> validate(env, c))
                .collect(Collectors.toList());
        return new SelectPlan(primary, columns);
    }
    
    static boolean hasOr(SqlValue v){
        if(!(v instanceof BinaryOp)){
            return false;
        }
        BinaryOp bin = (BinaryOp) v;
        switch(bin.op){
            case "and":
                return hasOr(bin.left) || hasOr(bin.right);
            case "or":
                return true;
            default:
                return false;
        }
    }
    
    static void andSerialize(List<SqlValue> ands, SqlValue v){
        if(!(v instanceof BinaryOp)){
            ands.add(v);
            return;
        }
        BinaryOp bin = (BinaryOp) v;
        switch(bin.op){
            case "and":
                andSerialize(ands, bin.left);
                andSerialize(ands, bin.right);
                break;
            case "or":
                throw new RuntimeException("or can not be optimized");
            default:
                ands.add(bin);
        }
    }
    
    public static SelectPlan optimize(Schema sc, SelectPlan plan){
        //whereがないなら最適化しない
        if(!(plan.from instanceof FilterPlan)){
            return plan;
        }
        FilterPlan filter = (FilterPlan) plan.from;
        List<SqlValue> conds = filter.conds;
        //orが入ってたら最適化しない
        if(conds.stream().anyMatch(cond -> hasOr(cond))){
            return plan;
        }
        
        //プライマリテーブルを取得
        QueryPlan q = filter;
        for(;q instanceof NodePlan; q = ((NodePlan)q).from){
            // do nothing;
        }
        TablePlan t = (TablePlan) q;
        
        //andをリストに分解
        List<SqlValue> ands = new ArrayList<>();
        conds.stream()
                .forEach(cond -> andSerialize(ands, cond));
        
        boolean alwaysFalse = false;
        List<SqlValue> root = new ArrayList<>();
        for(Iterator<SqlValue> ite = ands.iterator(); ite.hasNext();){
            SqlValue v = ite.next();
            if(v instanceof BinaryOp){
                BinaryOp bin = (BinaryOp) v;
                //定数同士での比較はあらかじめ計算する
                if(bin.left instanceof IntValue && bin.right instanceof IntValue){
                    SqlValue b = eval(v, null, null);
                    if(b instanceof BooleanValue){
                        if(((BooleanValue)b).value){
                            //常に真
                            ite.remove();//計算不要
                            continue;
                        }else{
                            //常に偽
                            alwaysFalse = true;
                            break;
                        }
                    }
                }
                //片方がフィールドのとき
                if((bin.left instanceof FieldValue && !(bin.right instanceof FieldValue)) ||
                        (!(bin.left instanceof FieldValue) && bin.right instanceof FieldValue)){
                    FieldValue f = (FieldValue)((bin.left instanceof FieldValue) ? bin.left : bin.right);
                    f.column.parent
                            .filter(pt -> pt.name.equals(t.table.name))
                            .ifPresent(pt -> 
                    {
                        root.add(bin);
                        ite.remove();
                    });
                }
                        
            }
        }
        
        if(alwaysFalse){
            //常に偽
            ands.clear();
            root.clear();
            //テーブルの前にemptyフィルター
            Optional<NodePlan> oto = t.to;
            EmptyPlan ep = new EmptyPlan(t);
            ep.to = oto;
            oto.ifPresent(to -> to.from = ep);
        }
        
        if(ands.isEmpty()){
            //もとの条件式が空になったらfilterをはずす
            filter.from.to = filter.to;
            filter.to.ifPresent(to -> to.from = filter.from);
        }else{
            //空でなければfilterいれかえ
            FilterPlan newFilter = new FilterPlan(filter.from, ands);
            filter.to.ifPresent(to -> to.from = newFilter);
            newFilter.to = filter.to;
        }
        
        if(!root.isEmpty()){
            //テーブルフィルターが空でなければfilter挿入
            Optional<NodePlan> oto = t.to;
            FilterPlan newFilter = new FilterPlan(t, root);
            newFilter.to = oto;
            oto.ifPresent(to -> to.from = newFilter);
        }
        
        return plan;
    }
    
    public static void insert(Schema sc, ASTInsert insert){
        Table t = sc.find(insert.table.ident)
                .orElseThrow(() -> new RuntimeException(
                        String.format("table %s not found.", insert.table.ident)));
        
        Counter c = new Counter();
        Map<String, Integer> cols = t.columns.stream()
                .collect(Collectors.toMap(col -> col.name, col -> c.getCount() - 1));
        int[] indexes;
        if(insert.field.isPresent()){
            indexes = insert.field.get().stream().mapToInt(id -> cols.get(id.ident)).toArray();
        }else{
            indexes = IntStream.range(0, t.columns.size()).toArray();
        }
        insert.value.stream().forEach(ro -> {
            Object[] row = new Object[t.columns.size()];
            for(int i = 0; i < ro.size(); ++i){
                row[indexes[i]] = unwrap(validate(null, ro.get(i))).orElse(null);
            }
            t.insert(row);
        });
    }
    
    public static void delete(Schema sc, ASTDelete del){
        Table t = sc.find(del.table.ident)
                .orElseThrow(() -> new RuntimeException(
                        String.format("table %s not found.", del.table.ident)));
        Map<String, Table> env = new HashMap<>();
        env.put(del.table.ident, t);
        QueryPlan primary = new TablePlan(t);

        if(del.where.isPresent()){
            SqlValue cond = del.where.map(a -> validate(env, a)).get();
            List<SqlValue> ands = new ArrayList<>();
            if(hasOr(cond)){
                ands.add(cond);
            }else{
                andSerialize(ands, cond);
            }
            primary = new FilterPlan(primary, ands);
        }
        Records<Tuple> rec = primary.records();
        List<Tuple> deletes = new ArrayList<>();
        for(Optional<Tuple> line; (line = rec.next()).isPresent(); ){
            deletes.add(line.get());
        }
        t.delete(deletes);
    }
    public static void update(Schema sc, ASTUpdate update){
        Table t = sc.find(update.table.ident)
                .orElseThrow(() -> new RuntimeException(
                        String.format("table %s not found.", update.table.ident)));
        Map<String, Table> env = new HashMap<>();
        env.put(update.table.ident, t);
        QueryPlan primary = new TablePlan(t);

        if(update.where.isPresent()){
            SqlValue cond = update.where.map(a -> validate(env, a)).get();
            List<SqlValue> ands = new ArrayList<>();
            if(hasOr(cond)){
                ands.add(cond);
            }else{
                andSerialize(ands, cond);
            }
            primary = new FilterPlan(primary, ands);
        }
        Counter c = new Counter();
        Map<String, Integer> cols = t.columns.stream()
                .collect(Collectors.toMap(col -> col.name, col -> c.getCount() - 1));
        List<Map.Entry<Integer, SqlValue>> values = 
                update.values.stream().map(v -> new AbstractMap.SimpleEntry<>(
                        cols.get(v.field.ident), validate(env, v.value))).collect(Collectors.toList());
        Map<Column, Integer> colIdx = primary.getColumnIndex();
        Records<Tuple> rec = primary.records();
        for(Optional<Tuple> oline; (oline = rec.next()).isPresent(); ){
            Tuple line = oline.get();
            List<Optional<?>> copy = new ArrayList<>(line.row);
            while(copy.size() < t.columns.size()){
                copy.add(Optional.empty());
            }
            
            values.stream().forEach(me -> {
                copy.set(me.getKey(), unwrap(eval(me.getValue(), colIdx, line)));
            });
            t.update(line.rid, copy);
        }
        
    }
    public static void exec(Schema sc, String sqlstr){
        Parser<SqlParser.ASTStatement> parser = SqlParser.parser();
        SqlParser.AST sql = parser.parse(sqlstr);
        if(sql instanceof ASTInsert){
            insert(sc, (ASTInsert) sql);
            return;
        }else if(sql instanceof ASTUpdate){
            update(sc, (ASTUpdate)sql);
            return;
        }else if(sql instanceof ASTDelete){
            delete(sc, (ASTDelete) sql);
            return;
        }else if(!(sql instanceof ASTSelect)){
            return;
        }
        ASTSelect select = (ASTSelect) sql;
        SelectPlan plan = analize(sc, select);
        System.out.println(sqlstr);
        System.out.println("初期プラン:" + plan);
        plan = optimize(sc, plan);
        System.out.println("論理最適化:" + plan);
        
        Records<Tuple> rec = plan.records();
        for(Optional<Tuple> line; (line = rec.next()).isPresent();){
            line.ifPresent(l -> {
                System.out.println(l.row.stream()
                        .map(o -> o.map(v -> v.toString()).orElse("null"))
                        .collect(Collectors.joining(",", "[", "]")));
            });
        }
        System.out.println();
    }
    
    public static void main(String[] args) {
        Table tshohin = new Table("shohin", Stream.of("id", "name", "bunrui_id", "price")
                .map(s -> new Column(s)).collect(Collectors.toList()));
        Table tbunrui = new Table("bunrui", Stream.of("id", "name", "seisen")
                .map(s -> new Column(s)).collect(Collectors.toList()));
        tbunrui
            .insert(1, "野菜", 1)
            .insert(2, "くだもの", 1)
            .insert(3, "菓子", 2)
            .insert(9, "団子");
        tshohin
            .insert(1, "りんご", 2, 250)
            .insert(2, "キャベツ", 1, 200)
            .insert(3, "たけのこの", 3, 150)
            .insert(4, "きのこ", 3, 120)
            .insert(5, "パソコン", 0, 34800)
            .insert(6, "のこぎり");
        
        Schema sc = new Schema(Arrays.asList(tshohin, tbunrui));
        exec(sc, "insert into bunrui values(4, '周辺機器', 2)");
        exec(sc, "insert into bunrui(name, id) values('酒', 5 )");
        exec(sc, "insert into bunrui(id, name) values(6, 'ビール' )");
        exec(sc, "insert into bunrui(id, name, seisen) values(7, '麺', 2), (8, '茶', 2)");
        exec(sc, "select * from bunrui");
        
        exec(sc, "select id,name,price,price*2 from shohin");
        exec(sc, "select id, name from shohin where price between 130 and 200 or id=1");
        exec(sc, "select id, name from shohin where price between 130 and 200");
        System.out.println("普通のJOIN");
        exec(sc, "select shohin.id, shohin.name,bunrui.name"
                + " from shohin left join bunrui on shohin.bunrui_id=bunrui.id");
        System.out.println("常に真なので条件省略");
        exec(sc, "select id, name from shohin where 2 < 3");
        System.out.println("常に偽なので空になる");
        exec(sc, "select id, name from shohin where price < 130 and 2 > 3");
        System.out.println("メインテーブルのみに関係のある条件はJOINの前に適用");
        exec(sc, "select shohin.id, shohin.name,bunrui.name"
                + " from shohin left join bunrui on shohin.bunrui_id=bunrui.id"
                + " where shohin.price <= 300 and bunrui.seisen=1");
        System.out.println("update");
        exec(sc, "update shohin set price=1500 where id=6");
        exec(sc, "update shohin set price=price+500 where id=5");
        exec(sc, "select * from shohin where id=6 or id=5");
        exec(sc, "update shohin set price=price*105/100");
        exec(sc, "select * from shohin");
        System.out.println("削除");
        exec(sc, "delete from bunrui where id=9");
        exec(sc, "select * from bunrui");
        System.out.println("全削除");
        exec(sc, "delete from bunrui");
        exec(sc, "select * from bunrui");
    }
}
