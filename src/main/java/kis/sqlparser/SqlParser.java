/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.codehaus.jparsec.OperatorTable;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.Terminals;

/**
 *
 * @author naoki
 */
public class SqlParser {
    static final String[] keywords = {
        "between", "and", "or", "select", "from", "left", "join", "on", "where", "insert", "into", "values", "update", "set"
    };
    
    static final String[] operators = {
        "=", "<", ">", "<=", ">=", ".", "*", ",", "(", ")"
    };
    
    static final Terminals terms = Terminals.caseInsensitive(operators, keywords);
    static Parser<Void> ignored = Scanners.WHITESPACES.optional();
    static Parser<?> tokenizer = Parsers.or(
            terms.tokenizer(),
            Terminals.Identifier.TOKENIZER,
            Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
            Terminals.IntegerLiteral.TOKENIZER);
    
    public interface AST{}
    
    // integer
    @AllArgsConstructor @ToString
    public static class ASTInt implements AST{
        int value;
    }
    public static Parser<ASTInt> integer(){
        return Terminals.IntegerLiteral.PARSER.map(s -> new ASTInt(Integer.parseInt(s)));
    }
    
    // identifier
    @AllArgsConstructor @ToString
    public static class ASTIdent implements AST{
        String ident;
    }
    public static Parser<ASTIdent> identifier(){
        return Terminals.Identifier.PARSER.map(s -> new ASTIdent(s));
    }
    
    // str
    @AllArgsConstructor @ToString
    public static class ASTStr implements AST{
        String str;
    }
    public static Parser<ASTStr> str(){
        return Terminals.StringLiteral.PARSER.map(s -> new ASTStr(
                s.replaceAll("''", "'")));
    }
    
    // fqn := identifier "." identifier
    @AllArgsConstructor @ToString
    public static class ASTFqn implements AST{
        ASTIdent table;
        ASTIdent field;
    }
    public static Parser<ASTFqn> fqn(){
        return identifier().next(t -> terms.token(".").next(identifier()).map(f -> new ASTFqn(t, f)));
    }
    
    // value := fqn | identifier | integer | str
    public static Parser<AST> value(){
        return Parsers.or(fqn(), identifier(), integer(), str());
    }
    
    // bicond := value ("=" | "<" | "<=" | ">" | ">=) value
    @AllArgsConstructor @ToString
    public static class ASTCond implements AST{
        AST left;
        AST right;
        String op;
    }
    
    public static Parser<ASTCond> bicond(){
        return value().next(l -> 
                terms.token("=", "<", "<=", ">", ">=").source()
                        .next(op -> 
                value().map(r -> new ASTCond(l, r, op))));
    }
    
    // between := value "between" value "and" value
    @AllArgsConstructor @ToString
    public static class ASTBetween implements AST{
        AST obj;
        AST start;
        AST end;
    }
    
    public static Parser<ASTBetween> between(){
        return value().next(o ->
            terms.token("between").next(value()).next(st -> 
                    terms.token("and").next(value()).map(ed -> 
                            new ASTBetween(o, st, ed))));
    }
    // cond := bicond | between
    public static Parser<AST> cond(){
        return Parsers.or(bicond(), between());
    }
    
    // logic := cond (("and" | "or") cond)*
    @AllArgsConstructor @ToString
    public static class ASTLogic implements AST{
        AST left;
        AST right;
        String op;
    }
    @AllArgsConstructor @ToString
    static class Cont{
        String op;
        AST cond;
    }
    
    public static Parser<AST> logic(){
        return new OperatorTable<AST>()
                .infixl(terms.token("and").retn((l, r) -> new ASTLogic(l, r, "and")), 1)
                .infixl(terms.token("or").retn((l, r) -> new ASTLogic(l, r, "or")), 1)
                .build(cond());
    }
    
    // select := "select" value ("," value)*
    @AllArgsConstructor @ToString
    public static class ASTSelect implements AST{
        List<? extends AST> cols;
    }
    
    public static class ASTWildcard implements AST{
    }
    
    public static Parser<ASTSelect> select(){
        return terms.token("select").next(Parsers.or(
                terms.token("*").map(t -> Arrays.asList(new ASTWildcard())), 
                value().sepBy1(terms.token(","))
        )).map(l -> new ASTSelect(l));
    }
    
    // table := identifier
    // field := identifier | fqn
    // join := "left" "join" table "on" logic
    @AllArgsConstructor @ToString
    public static class ASTJoin implements AST{
        ASTIdent table;
        AST logic;
    }
    
    public static Parser<ASTJoin> join(){
        return terms.phrase("left", "join")
                .next(identifier().next(t -> terms.token("on")
                        .next(logic()).map(lg -> new ASTJoin(t, lg))));
    }
    
    // from := "from" table join*
    @AllArgsConstructor @ToString
    public static class ASTFrom implements AST{
        ASTIdent table;
        List<ASTJoin> joins;
    }
    
    public static Parser<ASTFrom> from(){
        return terms.token("from").next(identifier()
                .next(t -> join().many().map(j -> new ASTFrom(t, j))));
    }
    // where := "where" logic
    public static Parser<AST> where(){
        return terms.token("where").next(logic());
    }
    
    // selectStatement := select from where?
    @AllArgsConstructor @ToString
    public static class ASTSql implements AST{
        ASTSelect select;
        ASTFrom from;
        Optional<? extends AST> where;
    }
    public static Parser<ASTSql> selectStatement(){
        return select().next(s -> from().next(f -> where().optional()
                .map(w -> new ASTSql(s, f, Optional.ofNullable(w)))));
    }

    // insertField := "(" identity ("," identity)* ")"
    public static Parser<List<ASTIdent>> insertField(){
        return Parsers.between(
                terms.token("("), 
                identifier().sepBy1(terms.token(",")) , 
                terms.token(")"));
    }
    
    public static Parser<List<AST>> insertValues(){
        return Parsers.between(
                terms.token("("), 
                Parsers.or(integer(), str()).sepBy1(terms.token(",")), 
                terms.token(")"));
    }
    
    // insert := "insert" "into" table insertField? "values" insertValues ("," insertValues)*
    @AllArgsConstructor @ToString
    public static class ASTInsert implements AST{
        ASTIdent table;
        Optional<List<ASTIdent>> field;
        List<List<AST>> value;
    }
    
    public static Parser<ASTInsert> insert(){
        return Parsers.sequence(
                terms.phrase("insert", "into").next(identifier()),
                insertField().optional(),
                terms.token("values").next(insertValues().sepBy1(terms.token(","))),
                (tb, f, v) -> new ASTInsert(tb, Optional.ofNullable(f), v));
    }
    
    @AllArgsConstructor
    public static class ASTDelete implements AST{
        ASTIdent table;
        Optional<AST> where;
    }
    
    public static Parser<ASTDelete> delete(){
        return Parsers.sequence(
                terms.token("delete").next(terms.token("*").optional())
                        .next(terms.token("from")).next(identifier()),
                where().optional(),
                (id, w) -> new ASTDelete(id, Optional.ofNullable(w)));
    }
    
    // insertValue := ident "=" value
    @AllArgsConstructor @ToString
    public static class ASTUpdateValue implements AST{
        ASTIdent field;
        AST value;
    }
    
    public static Parser<ASTUpdateValue> updateValue(){
        return Parsers.sequence(
                identifier(),
                terms.token("=").next(value()),
                (id, v) -> new ASTUpdateValue(id, v));
    }
    
    // update := "update" table "set" updateValue ("," updateValue)* where
    @AllArgsConstructor @ToString
    public static class ASTUpdate implements AST{
        ASTIdent table;
        List<ASTUpdateValue> values;
        Optional<AST> where;
    }
    
    public static Parser<ASTUpdate> update(){
        return Parsers.sequence(
                terms.token("update").next(identifier()),
                terms.token("set").next(updateValue().sepBy1(terms.token(","))),
                where().optional(),
                (tbl, values, where) -> new ASTUpdate(tbl, values, Optional.ofNullable(where)));
    }
    
    
    public static Parser<AST> parser(){
        return Parsers.or(selectStatement(), insert()).from(tokenizer, ignored);
    }

}
