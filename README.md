sqlparser
=========

tiny sql parser

execute kis.sqlparser.SqlAnalizer

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
            .insert(3, "たけのこ", 3, 150)
            .insert(4, "きのこ", 3, 120);
        
        Schema sc = new Schema(Arrays.asList(tshohin, tbunrui));
        print(sc, "select id, name from shohin where price between 130 and 200 or id=1");
        print(sc, "select shohin.id, shohin.name,bunrui.name"
                + " from shohin left join bunrui on shohin.bunrui_id=bunrui.id");
    }

result

    select id, name from shohin where price between 130 and 200 or id=1
    [1,りんご]
    [2,キャベツ]
    [3,たけのこ]
    select shohin.id, shohin.name,bunrui.name from shohin left join bunrui on shohin.bunrui_id=bunrui.id
    [1,りんご,くだもの]
    [2,キャベツ,野菜]
    [3,たけのこ,菓子]
    [4,きのこ,菓子]

