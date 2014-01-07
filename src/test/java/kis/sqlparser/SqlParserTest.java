/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

import java.util.List;
import org.codehaus.jparsec.Parser;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author naoki
 */
public class SqlParserTest {
    @Test
    public void 整数(){
        System.out.println(SqlParser.integer().parse("123"));
    }
    
    @Test
    public void 識別子(){
        System.out.println(SqlParser.identifier().parse("shohin"));
    }
    
    @Test
    public void 文字列(){
        Parser<SqlParser.StringValue> parser = SqlParser.str().from(SqlParser.tokenizer, SqlParser.ignored);
        assertThat(parser.parse("'test'").value, is("test"));
        assertThat(parser.parse("''").value , is(""));
        assertThat(parser.parse("'tes''t'").value, is("tes't"));
        assertThat(parser.parse("'tes''t'''").value, is("tes't'"));
    }
    
    @Test
    public void 完全名(){
        System.out.println(SqlParser.fqn().from(SqlParser.tokenizer, SqlParser.ignored).parse("shohin.price"));
    }
    
    @Test
    public void 比較(){
        Parser<SqlParser.ASTBinaryOp> parser = SqlParser.bicond().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("shohin.bunrui_id=bunrui.id"));
        System.out.println(parser.parse("shohin . bunrui_id = bunrui . id"));
        System.out.println(parser.parse("shohin.bunrui_id <= 12"));
    }
    
    @Test
    public void betweentest(){
        System.out.println(SqlParser.between().from(SqlParser.tokenizer, SqlParser.ignored).parse("shohin.price between 100 and 200"));
    }
    
    @Test
    public void 論理(){
        System.out.println(SqlParser.logic().from(SqlParser.tokenizer, SqlParser.ignored).parse("bunrui.id=3 and shohin.price between 100 and 200 or bunrui.id=4"));
    }
    
    @Test
    public void select句(){
        Parser<SqlParser.ASTSelect> parser = SqlParser.select().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("select *"));
        System.out.println(parser.parse("select id, name"));
        System.out.println(parser.parse("select id"));
    }
    
    @Test
    public void selectsql全体(){
        Parser<SqlParser.ASTSql> parser = SqlParser.selectStatement().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("select * from shohin where id=3"));
        System.out.println(parser.parse("select * from shohin left join bunrui on shohin.bunrui_id=bunrui.id where id=3"));
        System.out.println(parser.parse("select id, name from shohin"));
    }
    
    @Test
    public void insertField(){
        Parser<List<SqlParser.ASTIdent>> parser = SqlParser.insertField().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("(id, name)"));
    }
    
    @Test
    public void insertValue(){
        Parser<List<SqlParser.ASTExp>> parser = SqlParser.insertValues().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("(1, 'test')"));
        System.out.println(parser.parse("(3, 'test', 2, 'hoge')"));
    }
    
    @Test
    public void insert(){
        Parser<SqlParser.ASTInsert> parser = SqlParser.insert().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("insert into shohin(id, name) values(1, 'hoge')"));
        System.out.println(parser.parse("insert into shohin values(1, 'hoge')"));
        System.out.println(parser.parse("insert into shohin values(1, 'hoge'), (2, 'foo')"));
        
    }
    @Test
    public void updateValue(){
        Parser<SqlParser.ASTUpdateValue> parser = SqlParser.updateValue().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("id=23"));
        System.out.println(parser.parse("name='abc'"));
    }
    
    @Test
    public void update(){
        Parser<SqlParser.ASTUpdate> parser = SqlParser.update().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("update shohin set name='test'"));
        System.out.println(parser.parse("update shohin set name='test' where id=2"));
        System.out.println(parser.parse("update shohin set name='test', seisen=1"));
        System.out.println(parser.parse("update shohin set name='test', seisen=1 where id=2"));
        System.out.println(parser.parse("update shohin set name='test', price=price+1 where id=2"));
    }
    
    @Test
    public void expression(){
        Parser<SqlParser.ASTExp> parser = SqlParser.expression().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("12+3*4"));
        System.out.println(parser.parse("price+3*4"));
        System.out.println(parser.parse("bunrui.price+3*4"));
        System.out.println(parser.parse("12"));
        System.out.println(parser.parse("price"));
        System.out.println(parser.parse("bunrui.price"));
    }
}
