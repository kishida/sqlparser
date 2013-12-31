sqlparser
=========

tiny sql parser

execute kis.sqlparser.SqlAnalizer

    Table tshohin = new Table("shohin", Stream.of("id", "name", "bunrui_id", "price")
            .map(s -> new Column(s)).collect(Collectors.toList()));
    Table tbunrui = new Table("bunrui", Stream.of("id", "name", "seisen")
            .map(s -> new Column(s)).collect(Collectors.toList()));
    tbunrui
        .insert(1, "野菜", 1)
        .insert(2, "くだもの", 1)
        .insert(3, "菓子", 2);
    tshohin
        .insert(1, "りんご", 2, 250)
        .insert(2, "キャベツ", 1, 200)
        .insert(3, "たけのこの", 3, 150)
        .insert(4, "きのこ", 3, 120)
        .insert(5, "パソコン", 0, 34800);

result

    select id, name from shohin where price between 130 and 200 or id=1
    初期プラン:select
      <- filter[between shohin.price:130:200 or shohin.id = 1]
      <- table[shohin]
    論理最適化:select
      <- filter[between shohin.price:130:200 or shohin.id = 1]
      <- table[shohin]
    [1,りんご]
    [2,キャベツ]
    [3,たけのこの]
    
    select id, name from shohin where price between 130 and 200
    初期プラン:select
      <- filter[between shohin.price:130:200]
      <- table[shohin]
    論理最適化:select
      <- filter[between shohin.price:130:200]
      <- table[shohin]
    [2,キャベツ]
    [3,たけのこの]

    普通のJOIN
    select shohin.id, shohin.name,bunrui.name
      from shohin left join bunrui on shohin.bunrui_id=bunrui.id
    初期プラン:select
      <- join(nested loop)
      <- table[shohin]
      /
      <- table[bunrui]
    論理最適化:select
      <- join(nested loop)
      <- table[shohin]
      /
      <- table[bunrui]
    [1,りんご,くだもの]
    [2,キャベツ,野菜]
    [3,たけのこの,菓子]
    [4,きのこ,菓子]
    [5,パソコン,null]

optimize

    常に真なので条件省略
    select id, name from shohin where 2 < 3
    初期プラン:select
      <- filter[2 < 3]
      <- table[shohin]
    論理最適化:select
      <- table[shohin]
    [1,りんご]
    [2,キャベツ]
    [3,たけのこの]
    [4,きのこ]
    [5,パソコン]

    常に偽なので空になる
    select id, name from shohin where price < 130 and 2 > 3
    初期プラン:select
      <- filter[shohin.price < 130 and 2 > 3]
      <- table[shohin]
    論理最適化:select
      <- empty
      <- table[shohin]

    メインテーブルのみに関係のある条件はJOINの前に適用
    select shohin.id, shohin.name,bunrui.name
      from shohin left join bunrui on shohin.bunrui_id=bunrui.id
      where shohin.price <= 300 and bunrui.seisen=1
    初期プラン:select
      <- filter[shohin.price <= 300 and bunrui.seisen = 1]
      <- join(nested loop)
      <- table[shohin]
      /
      <- table[bunrui]
    論理最適化:select
      <- filter[bunrui.seisen = 1]
      <- join(nested loop)
      <- filter[shohin.price <= 300]
      <- table[shohin]
      /
      <- table[bunrui]
    [1,りんご,くだもの]
    [2,キャベツ,野菜]


