/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.joining;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static kis.sqlparser.ObjectPatternMatch.*;
import static kis.sqlparser.StreamUtils.zip;
import kis.sqlparser.SqlParser.*;
import kis.sqlparser.Table.Tuple;
import kis.sqlparser.Table.TableTuple;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.codehaus.jparsec.Parser;

/**
 *
 * @author naoki
 */
public class SqlAnalizer {
    public static interface SqlValue{// extends ASTExp{
        
    }
    @AllArgsConstructor @EqualsAndHashCode
    public static class BooleanValue implements SqlValue{
        boolean value;
        
        @Override
        public String toString() {
            return value + "";
        }
    }
    @EqualsAndHashCode
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
    public static class FieldIndex implements SqlValue{
        int idx;
        
        @Override
        public String toString() {
            return "Field." + idx;
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
    @ToString
    public static abstract class FunctionExp implements SqlValue{
        String name;
        List<SqlValue> params;

        public FunctionExp(String name, List<SqlValue> params, int paramCount) {
            this.name = name;
            this.params = params;
            if(params.size() != paramCount){
                throw new RuntimeException(name + " function requires " + paramCount + " param");
            }
        }
        
    }    
    public static abstract class GeneralFuncExp extends FunctionExp{
        public GeneralFuncExp(String name, List<SqlValue> params, int count) {
            super(name, params, count);
        }
        
        abstract SqlValue eval(Map<Column, Integer> colIndex, Tuple tuple);
    }
    public static class LengthFunc extends GeneralFuncExp{
        public LengthFunc(List<SqlValue> params) {
            super("length", params, 1);
        }
        
        @Override
        SqlValue eval(Map<Column, Integer> colIndex, Tuple tuple) {
            SqlValue result = SqlAnalizer.eval(params.get(0), colIndex, tuple);
            if(result instanceof StringValue){
                return new IntValue(((StringValue)result).value.length());
            }
            throw new RuntimeException(result.getClass() + " is not supported for length()");
        }
    }
    public static class NowFunc extends GeneralFuncExp{

        public NowFunc(List<SqlValue> params) {
            super("now", params, 0);
        }

        @Override
        SqlValue eval(Map<Column, Integer> colIndex, Tuple tuple) {
            return new StringValue(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")));
        }
        
    }
    public static abstract class AggregationExp extends FunctionExp{
        public AggregationExp(String name, List<SqlValue> param, int paramCount) {
            super(name, param, paramCount);
        }
        abstract void accept(Map<Column, Integer> colIndex, Tuple tuple);
        abstract SqlValue getValue();
        abstract void reset();
    }
    
    public static class CountExp extends AggregationExp{
        int count;

        public CountExp(List<SqlValue> param) {
            super("count", param, 1);
            count = 0;
        }
        @Override
        void accept(Map<Column, Integer> colIndex, Tuple tuple) {
            SqlValue result = eval(params.get(0), colIndex, tuple);
            if(!(result instanceof NullValue)){
                ++count;
            }
        }

        @Override
        void reset() {
            count = 0;
        }
        
        @Override
        SqlValue getValue() {
            return new IntValue(count);
        }
    }

    public static class SumExp extends AggregationExp{
        int total;

        public SumExp(List<SqlValue> params) {
            super("sum", params, 1);
            total = 0;
        }
        @Override
        void accept(Map<Column, Integer> colIndex, Tuple tuple) {
            SqlValue result = eval(params.get(0), colIndex, tuple);
            if(result instanceof IntValue){
                total += ((IntValue)result).value;
            }else if(result instanceof NullValue){
            }else{
                throw new RuntimeException(result.getClass() + " is not supported for sum()");
            }
        }

        @Override
        void reset() {
            total = 0;
        }

        @Override
        SqlValue getValue() {
            return new IntValue(total);
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
            caseOfRet(GeneralFuncExp.class, f -> f.eval(colIndex, tuple)),
            caseOfRet(AggregationExp.class, a -> a.getValue()),
            caseOfRet(FieldValue.class, f -> {
                int idx = colIndex.getOrDefault(f.column, -1);
                if(idx < 0) throw new RuntimeException(f.column + " not found.");
                if(idx >= tuple.row.size()) return new NullValue();
                return wrap(tuple.row.get(idx));
            }),
            caseOfRet(FieldIndex.class, f -> {
                if(f.idx >= tuple.row.size()) return new NullValue();
                return wrap(tuple.row.get(f.idx));
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
                .collect(toList());
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
            return StreamUtils.zip(getColumns().stream(), Stream.iterate(0, i -> i + 1))
                    .collect(toMap(p -> p.left, p -> p.right));
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
    public static abstract class ColumnThroughPlan extends NodePlan{
        public ColumnThroughPlan(QueryPlan from) {
            super(from);
        }
        @Override
        List<Column> getColumns() {
            return from.getColumns();
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
    public static interface Records<T>{
        Optional<T> next();
    }
    
    public static class SelectPlan extends NodePlan{
        List<? extends SqlValue> values;
        
        public SelectPlan(QueryPlan from, List<? extends SqlValue> values){
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
                .collect(toList());
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
                        .collect(toList());
                return Optional.of(new Tuple(line.get().rid, row));
            };        
        }

        @Override
        public String toString() {
            return "select\n  <- " + from.toString();
        }
        
    }

    public static class FilterPlan extends ColumnThroughPlan{
        List<SqlValue> conds;

        public FilterPlan(QueryPlan from, List<SqlValue> conds) {
            super(from);
            this.conds = conds;
        }
        public FilterPlan(QueryPlan from, SqlValue cond) {
            this(from, Arrays.asList(cond));
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
    
    public static class EmptyPlan extends ColumnThroughPlan{

        public EmptyPlan(QueryPlan from) {
            super(from);
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

    public static class TablePlan extends QueryPlan{
        Table table;
        Optional<Transaction> tx;

        public TablePlan(Optional<Transaction> tx, Table table) {
            this.table = table;
            this.tx = tx;
        }

        @Override
        List<Column> getColumns() {
            return table.columns;
        }

        @Override
        Records<Tuple> records() {
            Iterator<TableTuple> ite = table.data.values().iterator();
            return () -> {
                while(ite.hasNext()) {
                    TableTuple tuple = ite.next();
                    if(tx.isPresent()){
                        //トランザクションがある
                        if(!tx.get().tupleAvailable(tuple)) continue;
                    }else{
                        //トランザクションがない
                        if(!tuple.isCommited()) continue;
                    }
                    //if(tx.isPresent() && )
                    return Optional.of(tuple);
                }
                return Optional.empty();
            };
        }

        @Override
        public String toString() {
            return "table[" + table.name + "]";
        }
    }

    public static class HistoryPlan extends QueryPlan{
        Optional<Transaction> tx;
        Table table;

        public HistoryPlan(Optional<Transaction> tx, Table table) {
            this.table = table;
            this.tx = tx;
        }

        @Override
        List<Column> getColumns() {
            return table.columns;
        }

        @Override
        Records<Tuple> records() {
            Iterator<TableTuple> ite = table.getModifiedTuples(tx).iterator();
            return () ->
                ite.hasNext() ? Optional.of(ite.next()) : Optional.empty();
        }
    }
    
    public static class UnionPlan extends NodePlan{
        QueryPlan secondPlan;

        public UnionPlan(QueryPlan firstPlan, QueryPlan secondPlan) {
            super(firstPlan);
            this.secondPlan = secondPlan;
        }

        @Override
        List<Column> getColumns() {
            return from.getColumns();
        }

        @Override
        Records<Tuple> records() {
            List<Records<Tuple>> recs = Arrays.asList(
                    from.records(), secondPlan.records());
            int[] idx = {0};
            return () -> {
                while(idx[0] < recs.size()){
                    Optional<Tuple> rec = recs.get(idx[0]).next();
                    if(rec.isPresent()) return rec;
                    ++idx[0];
                }
                return Optional.empty();
            };
        }
    }
    
    public static class OrderPlan extends ColumnThroughPlan{
        List<Pair<? extends SqlValue, Boolean>> order;
        
        public OrderPlan(QueryPlan from, List<Pair<? extends SqlValue, Boolean>> order) {
            super(from);
            this.order = order;
        }

        @Override
        Records<Tuple> records() {
            //タプルと、ソート用値列をまとめる
            List<Pair<Tuple, List<Pair<SqlValue, Boolean>>>> tuples = new ArrayList<>();
            Records<Tuple> rec = from.records();
            Map<Column, Integer> columnIndex = getColumnIndex();
            for(Optional<Tuple> line; (line = rec.next()).isPresent();){
                Tuple t = line.get();
                tuples.add(Pair.of(t, 
                        order.stream().map(p -> p.map(v -> eval(v, columnIndex, t), b -> b))
                                .collect(toList())));
            }
            tuples.sort((o1, o2) -> {
                for(Iterator<Pair<SqlValue, Boolean>> itel =o1.right.iterator(), iter = o2.right.iterator(); itel.hasNext() && iter.hasNext();){
                    Pair<SqlValue, Boolean> l = itel.next();
                    Pair<SqlValue, Boolean> r = iter.next();
                    if(l.left instanceof NullValue){
                        if(r.left instanceof NullValue){
                            continue;
                        }
                        return 1;
                    }
                    if(r.left instanceof NullValue){
                        return -1;
                    }
                    if(r.left instanceof IntValue && l.left instanceof IntValue){
                        int ret = Integer.compare(((IntValue)l.left).value, ((IntValue)r.left).value);
                        if(ret == 0) continue;
                        return l.right ? ret : -ret;
                    }
                    if(r.left instanceof StringValue && l.left instanceof StringValue){
                        int ret = ((StringValue)l.left).value.compareTo(((StringValue)r.left).value);
                        if(ret == 0) continue;
                        return l.right ? ret : -ret;
                    }
                }
                return 0;
            });
            Iterator<Pair<Tuple, List<Pair<SqlValue, Boolean>>>> ite = tuples.iterator();
            return () -> {
                return ite.hasNext() ? Optional.of(ite.next().left) : Optional.empty();
            };
        }

        @Override
        public String toString() {
            return "order[] <- " + from.toString();
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
                    secondary.getColumns().stream()).collect(toList());
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
                            .collect(toList()));
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
                        .collect(toList())));
            };
        }
        
        @Override
        public String toString() {
            return "join(nested loop)\n  <- " + from.toString() + "\n  /\n  <- " + secondary.toString();
        }

    }
    public static void gatherAggregation(List<AggregationExp> aggs, SqlValue v){
        match(v, 
            caseOf(GeneralFuncExp.class, f -> {
                f.params.forEach(p -> gatherAggregation(aggs, p));
            }),
            caseOf(BinaryOp.class, bin -> {
                gatherAggregation(aggs, bin.left);
                gatherAggregation(aggs, bin.right);
            }),
            caseOf(TernaryOp.class, ter -> {
                gatherAggregation(aggs, ter.cond);
                gatherAggregation(aggs, ter.first);
                gatherAggregation(aggs, ter.sec);
            }),
            caseOf(AggregationExp.class, agg ->{
                aggs.add(agg);
            })
        );
    }
    static final UnaryOperator<Integer> INC = i -> i + 1;
    public static class AggregationPlan extends NodePlan{
        List<SqlValue> group;
        List<SqlValue> fields;
        public AggregationPlan(QueryPlan from, List<SqlValue> fields, List<SqlValue> group) {
            super(from);
            this.fields = fields;
            this.group = group;
        }
        
        @Override
        List<Column> getColumns() {
            return zip(fields.stream(), Stream.iterate(1, INC))
                    .map(p -> p.reduce((f, i) -> f instanceof FieldValue ? 
                            ((FieldValue)f).column : new Column(i + "")))
                    .collect(toList());
        }
    
        @Override
        Records<Tuple> records() {
            Records<Tuple> records = from.records();
            Map<Column, Integer> columnIndex = from.getColumnIndex();
            //集計関数を集める
            List<AggregationExp> aggs = new ArrayList<>();
            fields.forEach(f -> gatherAggregation(aggs, f));
            aggs.forEach(a -> a.reset());
            //集計行インデックス
            Map<Column, Integer> grpColIndex = zip(group.stream(),Stream.iterate(0, INC))
                    .map(p -> p.reduce((v, i) -> Pair.of(
                            v instanceof FieldValue ? ((FieldValue)v).column : 
                                    new Column(i + "") , i)))
                    .collect(toMap(p -> p.left, p -> p.right));
            //前の行の値(初期値Null)
            List<SqlValue> oldValue = IntStream.range(0, group.size())
                    .mapToObj(i -> new NullValue()).collect(toList());
            
            return new Records<Tuple>() {
                private boolean top = true;
                private boolean end = false;
                
                private Optional<Tuple> createResult(List<SqlValue> gvalues){
                    Tuple t = new Tuple(0, 
                            gvalues.stream().map(sv-> unwrap(sv)).collect(toList()));
                    List<Optional<?>> fv = fields.stream()
                            .map(f -> eval(f, grpColIndex, t))
                            .map(f -> unwrap(f))
                            .collect(toList());
                    return Optional.of(new Tuple(0, fv));
                }

                @Override
                public Optional<Tuple> next() {
                    if(end){
                        return Optional.empty();
                    }
                    for(;;){
                        Optional<Tuple> oline = records.next();
                        if(oline.isPresent()){
                            //まだある
                            Optional<Tuple> result = Optional.empty();
                            //group値を得る
                            List<SqlValue> gvalues = group.stream()
                                    .map(sv -> eval(sv, columnIndex, oline.get()))
                                    .collect(toList());
                            if(!group.isEmpty() && !top && //groupがあり先頭ではなく違う
                                    zip(gvalues.stream(), oldValue.stream())//oldValueとの比較
                                            .anyMatch(p -> !p.left.equals(p.right)))
                            {
                                //かわった
                                //結果生成
                                result = createResult(gvalues);
                                aggs.forEach(a -> a.reset());
                            }
                            top = false;
                            //oldValueの更新
                            zip(Stream.iterate(0, i -> i + 1), gvalues.stream())
                                    .forEach(p -> oldValue.set(p.left, p.right));
                            //集計
                            aggs.forEach(a -> a.accept(columnIndex, oline.get()));
                            if(result.isPresent()){
                                return result;
                            }
                        }else{
                            //もうおわり
                            end = true;
                            if(group.isEmpty() || !top){
                                //集計行がないか最初ではない
                                //残りの結果を返す
                                return createResult(oldValue);
                            }
                            return Optional.empty();
                        }
                    }
                }
            };
        }

        @Override
        public String toString() {
            return "agregate[]<-" + from.toString();
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
            caseOfRet(ASTFunc.class, f ->{
                List<SqlValue> params = f.params.stream().map(p -> validate(env, p)).collect(toList());
                switch(f.name.ident){
                    case "length":
                        return new LengthFunc(params);
                    case "now":
                        return new NowFunc(params);
                    case "count":
                        return new CountExp(params);
                    case "sum":
                        return new SumExp(params);
                    default:
                        throw new RuntimeException(f.name.ident + " function not defined.");
                }
            }),
            noMatchThrow(() -> 
                    new RuntimeException(ast.getClass().getName() + " is wrong type"))
        );
    }
    
    public static SelectPlan analize(Context ctx, SqlParser.ASTSelect sql){
        Map<String, Table> env = new HashMap<>();

        //From解析
        SqlParser.ASTFrom from = sql.from;
        
        Table table = ctx.schema.find(from.table.ident)
                .orElseThrow(() -> 
                        new RuntimeException("table " + from.table.ident + " not found"));
        env.put(from.table.ident, table);
        QueryPlan primary = new UnionPlan(
                new TablePlan(ctx.currentTx, table),
                new HistoryPlan(ctx.currentTx, table));
        
        for(ASTJoin j : from.joins){
            String t = j.table.ident;
            Table tb = ctx.schema.find(t).orElseThrow(() ->
                new RuntimeException("join table " + t + " not found"));
            env.put(t, tb);
            SqlValue cond = validate(env, j.logic);
            QueryPlan right = new UnionPlan(
                    new TablePlan(ctx.currentTx, tb),
                    new HistoryPlan(ctx.currentTx, tb));
            primary = new JoinPlan(primary, right, cond);
        }
        
        // where 解析
        if(sql.where.isPresent()){
            Optional<SqlValue> cond = sql.where.map(a -> validate(env, a));
            primary = new FilterPlan(primary, cond.get());
        }
        //group by
        if(!sql.groupby.isEmpty()){
            // selectでgroup byのフィールド以外が使われていないか走査
            List<SqlValue> group = sql.groupby.stream().map(gb -> validate(env, gb)).collect(toList());
            //集計用order byの挿入
            List<Pair<? extends SqlValue, Boolean>> order = group.stream().map(g -> Pair.of(g, true))
                    .collect(toList());
            primary = new OrderPlan(primary, order);
            //集計の挿入
            List<SqlValue> aggregation = Stream.of(
                    sql.select.stream().map(ast -> validate(env, ast)),
                    sql.order.stream().map(ast -> validate(env, ast.exp)),
                    sql.having.map(c -> validate(env, c)).map(sv -> Stream.of(sv)).orElse(Stream.empty())
            ).flatMap(s -> s).collect(toList());
            primary = new AggregationPlan(primary, aggregation, group);
            //having filter
            if(sql.having.isPresent()){
                primary = new FilterPlan(primary, new FieldIndex(sql.select.size() + sql.order.size()));
            }
            //order by
            if(!sql.order.isEmpty()){
                primary = new OrderPlan(primary, 
                        zip(sql.order.stream(), Stream.iterate(sql.select.size(), INC))
                    .map(p -> p.reduce((o, i) -> Pair.of(new FieldIndex(i), o.asc))).collect(toList()));
            }
            
            //select
            List<FieldIndex> columns = IntStream.range(0, sql.select.size())
                    .mapToObj(i -> new FieldIndex(i)).collect(toList());
            return new SelectPlan(primary, columns);
        }
        
        // order by
        if(!sql.order.isEmpty()){
            List<Pair<? extends SqlValue, Boolean>> order = sql.order.stream()
                    .map(ov -> Pair.of(validate(env, ov.exp), ov.asc))
                    .collect(toList());
            primary = new OrderPlan(primary, order);
        }
        
        //Select解析
        //todo 集計関数の確認
        List<SqlValue> columns = sql.select.stream()
                .map(c -> validate(env, c))
                .collect(toList());
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
        conds.forEach(cond -> andSerialize(ands, cond));
        
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
    
    public static int insert(Context ctx, ASTInsert insert){
        Table t = ctx.schema.find(insert.table.ident)
                .orElseThrow(() -> new RuntimeException(
                        String.format("table %s not found.", insert.table.ident)));
        
        Counter c = new Counter();
        Map<String, Integer> cols = t.columns.stream()
                .collect(toMap(col -> col.name, col -> c.getCount() - 1));
        int[] indexes;
        if(insert.field.isPresent()){
            indexes = insert.field.get().stream().mapToInt(id -> cols.get(id.ident)).toArray();
        }else{
            indexes = IntStream.range(0, t.columns.size()).toArray();
        }

        ctx.withTx(tx -> {
            insert.value.forEach(ro -> {
                Object[] row = new Object[t.columns.size()];
                for(int i = 0; i < ro.size(); ++i){
                    row[indexes[i]] = unwrap(validate(null, ro.get(i))).orElse(null);
                }
                t.insert(tx, row);
            });
        });
        return insert.value.size();
    }
    
    public static int delete(Context ctx, ASTDelete del){
        Table t = ctx.schema.find(del.table.ident)
                .orElseThrow(() -> new RuntimeException(
                        String.format("table %s not found.", del.table.ident)));
        Map<String, Table> env = new HashMap<>();
        env.put(del.table.ident, t);
        QueryPlan primary = new TablePlan(ctx.currentTx, t);

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
        List<TableTuple> deletes = new ArrayList<>();
        for(Optional<Tuple> line; (line = rec.next()).isPresent(); ){
            deletes.add((TableTuple)line.get());
        }
        ctx.withTx(tx -> t.delete(tx, deletes));
        return deletes.size();
    }
    public static int update(Context ctx, ASTUpdate update){
        Table t = ctx.schema.find(update.table.ident)
                .orElseThrow(() -> new RuntimeException(
                        String.format("table %s not found.", update.table.ident)));
        Map<String, Table> env = new HashMap<>();
        env.put(update.table.ident, t);
        QueryPlan primary = new TablePlan(ctx.currentTx, t);

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
                .collect(toMap(col -> col.name, col -> c.getCount() - 1));
        List<Map.Entry<Integer, SqlValue>> values = 
                update.values.stream().map(v -> new AbstractMap.SimpleEntry<>(
                        cols.get(v.field.ident), validate(env, v.value))).collect(toList());
        Map<Column, Integer> colIdx = primary.getColumnIndex();
        Records<Tuple> rec = primary.records();
        int[] ct = {0};
        ctx.withTx(tx -> {
            for(Optional<Tuple> oline; (oline = rec.next()).isPresent(); ){
                Tuple line = oline.get();
                List<Optional<?>> copy = new ArrayList<>(line.row);
                while(copy.size() < t.columns.size()){
                    copy.add(Optional.empty());
                }

                values.forEach(me -> {
                    copy.set(me.getKey(), unwrap(eval(me.getValue(), colIdx, line)));
                });
                t.update(tx, line.rid, copy);
                ++ct[0];
            }
        });
        return ct[0];
    }
    
    public static void createTable(Schema sc, ASTCreateTable ct){
        Table table = new Table(ct.tableName.ident, 
                ct.fields.stream().map(id -> new Column(id.ident)).collect(toList()));
        sc.tables.put(table.name, table);
    }
    
    public static void createIndex(Schema sc, ASTCreateIndex ci){
        String tblname = ci.table.map(t -> t.ident).orElseThrow(() -> 
                new RuntimeException("need table for index"));
        Table tbl = sc.find(tblname).orElseThrow(() -> 
                new RuntimeException("table not found:" + tblname));
        if(ci.field.size() != 1) throw new RuntimeException("multiple index field is not supported.");
        Pair<Column, Integer> colidx = zip(tbl.columns.stream(), Stream.iterate(0, INC))
                .filter(p -> p.left.name.equals(ci.field.get(0).ident))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("field is not found:" + ci.field.get(0).ident));

        String method = ci.method.map(m -> m.ident).orElse("tree");
        Index idx;
        switch(method){
            case "tree":
                idx = new Index.TreeIndex(colidx.right);
                break;
            case "hash":
                idx = new Index.HashIndex(colidx.right);
                break;
            default:
                throw new RuntimeException("index method " + method + " is not supported");
        }
        tbl.addIndex(colidx.left, idx);
    }
    
    public static class IntRecords implements Records<Tuple>{
        int value;
        boolean top;

        public IntRecords(int value) {
            this.value = value;
            top = true;
        }
        
        @Override
        public Optional<Tuple> next() {
            if(top){
                top = false;
                return Optional.of(new Tuple(0, Arrays.asList(Optional.of(value))));
            }else{
                return Optional.empty();
            }
        }
    }
    
    public static Records<Tuple> exec(Context ctx, String sqlstr){
        Parser<SqlParser.ASTStatement> parser = SqlParser.parser();
        SqlParser.AST sql = parser.parse(sqlstr);
        if(sql instanceof ASTInsert){
            return new IntRecords(insert(ctx, (ASTInsert) sql));
        }else if(sql instanceof ASTUpdate){
            return new IntRecords(update(ctx, (ASTUpdate)sql));
        }else if(sql instanceof ASTDelete){
            return new IntRecords(delete(ctx, (ASTDelete) sql));
        }else if(sql instanceof ASTCreateTable){
            createTable(ctx.schema, (ASTCreateTable)sql);
            return new IntRecords(0);
        }else if(sql instanceof ASTCreateIndex){
            createIndex(ctx.schema, (ASTCreateIndex)sql);
            return new IntRecords(0);
        }else if(!(sql instanceof ASTSelect)){
            throw new RuntimeException("not supported");
        }
        ASTSelect select = (ASTSelect) sql;
        SelectPlan plan = analize(ctx, select);
        System.out.println(sqlstr);
        System.out.println("初期プラン:" + plan);
        plan = optimize(ctx.schema, plan);
        System.out.println("論理最適化:" + plan);
        
        return plan.records();
    }
    
    static void printResult(Records<Tuple> result){
        for(Optional<Tuple> line; (line = result.next()).isPresent();){
            line.ifPresent(l -> {
                System.out.println(l.row.stream()
                        .map(o -> o.map(v -> v.toString()).orElse("null"))
                        .collect(joining(",", "[", "]")));
            });
        }
        System.out.println();
    }
    
    public static void main(String[] args) {
        Schema sc = new Schema();
        Context ctx = sc.createContext();
        Context ctx2 = sc.createContext();
        Context ctx3 = sc.createContext();
        ctx.exec("create table shohin(id, name, bunrui_id, price)");
        ctx.exec("create table bunrui(id, name, seisen)");
        
        ctx.begin();
        ctx.exec("insert into bunrui(id, name, seisen) values" +
                "(1, '野菜', 1)," +
                "(2, 'くだもの', 1)," +
                "(3, '菓子', 2)," +
                "(9, '団子', 0)");
        System.out.println("コミット前");
        printResult(ctx2.exec("select * from bunrui"));
        ctx3.begin();
        ctx.commit();
        System.out.println("コミット後");
        printResult(ctx2.exec("select * from bunrui"));
        System.out.println("コミット前に始まったトランザクション");
        printResult(ctx3.exec("select * from bunrui"));
        ctx.exec("insert into shohin(id, name, bunrui_id, price) values" +
                "(1, 'りんご', 2, 250),"+
                "(2, 'キャベツ', 1, 200),"+
                "(3, 'たけのこの', 3, 150),"+
                "(4, 'きのこ', 3, 120),"+
                "(5, 'パソコン', 0, 34800),"+
                "(6, 'のこぎり')");
        
        ctx.exec("create table member(id, name, address)");
        ctx.exec("insert into member values(1, 'きしだ', '福岡'), (2, 'ほうじょう', '京都')");
        printResult(ctx.exec("select * from member"));
        
        ctx.exec("insert into bunrui values(4, '周辺機器', 2)");
        ctx.exec("insert into bunrui(name, id) values('酒', 5 )");
        ctx.exec("insert into bunrui(id, name) values(6, 'ビール' )");
        ctx.exec("insert into bunrui(id, name, seisen) values(7, '麺', 2), (8, '茶', 2)");
        printResult(ctx.exec("select bunrui_id, sum(price), count(price), 2 + 3 from shohin group by bunrui_id having count(price)>0 order by sum(price)"));
        printResult(ctx.exec("select id,name, now(), length(name) from bunrui"));
        printResult(ctx.exec("select * from bunrui"));
        printResult(ctx.exec("select * from bunrui order by id desc"));
        printResult(ctx.exec("select * from bunrui order by seisen, id desc"));
        
        printResult(ctx.exec("select id,name,price,price*2 from shohin"));
        printResult(ctx.exec("select id, name from shohin where price between 130 and 200 or id=1"));
        printResult(ctx.exec("select id, name from shohin where price between 130 and 200"));
        System.out.println("普通のJOIN");
        printResult(ctx.exec("select shohin.id, shohin.name,bunrui.name"
                + " from shohin left join bunrui on shohin.bunrui_id=bunrui.id"));
        System.out.println("常に真なので条件省略");
        printResult(ctx.exec("select id, name from shohin where 2 < 3"));
        System.out.println("常に偽なので空になる");
        printResult(ctx.exec("select id, name from shohin where price < 130 and 2 > 3"));
        System.out.println("メインテーブルのみに関係のある条件はJOINの前に適用");
        printResult(ctx.exec("select shohin.id, shohin.name,bunrui.name"
                + " from shohin left join bunrui on shohin.bunrui_id=bunrui.id"
                + " where shohin.price <= 300 and bunrui.seisen=1"));
        System.out.println("update");
        ctx.exec("update shohin set price=1500 where id=6");
        ctx.exec("update shohin set price=price+500 where id=5");
        printResult(ctx.exec("select * from shohin where id=6 or id=5"));
        ctx.exec("update shohin set price=price*105/100");
        printResult(ctx.exec("select * from shohin"));
        System.out.println("削除");
        ctx.exec("delete from bunrui where id=9");
        printResult(ctx.exec("select * from bunrui"));
        System.out.println("全削除");
        ctx.exec("delete from bunrui");
        printResult(ctx.exec("select * from bunrui"));
    }
}
