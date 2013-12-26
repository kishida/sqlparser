/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package kis.sqlparser;

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
        Parser<SqlParser.ASTStr> parser = SqlParser.str().from(SqlParser.tokenizer, SqlParser.ignored);
        assertThat(parser.parse("'test'").str, is("test"));
        assertThat(parser.parse("''").str , is(""));
        assertThat(parser.parse("'tes''t'").str, is("tes't"));
        assertThat(parser.parse("'tes''t'''").str, is("tes't'"));
    }
    
    @Test
    public void 完全名(){
        System.out.println(SqlParser.fqn().from(SqlParser.tokenizer, SqlParser.ignored).parse("shohin.price"));
    }
    
    @Test
    public void 比較(){
        Parser<SqlParser.ASTCond> parser = SqlParser.bicond().from(SqlParser.tokenizer, SqlParser.ignored);
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
    public void sql全体(){
        Parser<SqlParser.ASTSql> parser = SqlParser.sql().from(SqlParser.tokenizer, SqlParser.ignored);
        System.out.println(parser.parse("select * from shohin where id=3"));
        System.out.println(parser.parse("select * from shohin left join bunrui on shohin.bunrui_id=bunrui.id where id=3"));
        System.out.println(parser.parse("select id, name from shohin"));
    }
}
