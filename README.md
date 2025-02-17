﻿
﻿# linepasswordparser


## 免责声明
1. 本工具仅面向合法授权的企业安全建设行为，请勿对非授权目标进行爬取行为。
2. 禁止对本软件实施逆向工程、反编译、试图破译源代码等行为。

如果发现上述禁止行为，将保留追究您法律责任的权利。

如您在使用本工具的过程中存在任何非法行为，您需自行承担相应后果，工具开发人员将不承担任何法律及连带责任。

在安装并使用本工具前，请您务必审慎阅读、充分理解各条款内容，限制、免责条款或者其他涉及您重大权益的条款可能会以加粗、加下划线等形式提示您重点注意。 除非您已充分阅读、完全理解并接受本协议所有条款，否则，请您不要安装并使用本工具。您的使用行为或者您以其他任何明示或者默示方式表示接受本协议的，即视为您已阅读并同意本协议的约束。


## 背景描述
随着naz.api 事件的爆发，很多安全同学才发现黑灰产还有这样的玩法。其实黑产一直有这样的链条，老毛子的窃密木马产业链非常猖獗，比如RedLine、Lumma这些木马就专门干这事儿。

土豪公司可以直接购买国外的几个情报公司的数据，就个人了解 IntelligenceX是可以提供全量数据的（当然，价格也不便宜）。已经清洗和处理后的数据可以找socradar或者hudsonrock了。

对于黑产链条还有很多其他的玩法，最火的肯定是洗币圈的钱包~~ 去找一些和钱相关的企业员工跟踪。
当然，从甲方公司的视角去看待： 窃密产业链利用木马窃密 —— 黑灰产获取url username password/cookie —— 从员工账密入侵 是一个非常有威胁的供给链。

对于没钱的公司，还是得薅羊毛，那么，不管是从暗网还是tg，市面上流传的大部分数据，都是经过处理的数据（并非原始病毒木马的结果数据），差不多一行一个url/username/password的样子。


## 功能概述
对·社工库·文件进行解析，提取url、username、password信息，存储到clickhouse中；

- parseimport.go 主要负责解析和导入到clickhouse；
- deduplication.go 是为了解决clickhouse去重判断的性能问题，采用bloomfilter的方式全量去重（可以定期运行）

## 说明
- 最开始我直接用clickhouse-client 直接配合分隔符导入，后来发现有各种奇葩的格式，就改用python解析，python单核的问题会导致解析比较慢，后来就改成了golang版本，当然golang也有它的问题，所以我这里面的解析逻辑做了一些性能上的妥协。
- 核心的解析代码在里面，后续应该不会迭代了。至于要写到文件、还行写到es，自行魔改
- 注意：一开始只是考虑自己用的，后来很多朋友说需要，就考虑开源了：1. 没有所谓的配置，都是硬编码；2. db自己修改；3. 根据自己机器的内存、性能做优化，里面数字相关的，都是可以优化的地方；

db建议
```
CREATE TABLE sgk.urluserpass
(
	`id` Int64,
    `username` String,
    `password` String,
    `url` String,
    `source` String,
    `sourcedate` Date,
    INDEX idx_username_password_url (username, password, url) TYPE minmax GRANULARITY 8192,
    INDEX idx_username (username) TYPE minmax GRANULARITY 8192,
    INDEX idx_url (url) TYPE minmax GRANULARITY 8192
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sourcedate)  -- 以月份为单位进行分区
PRIMARY KEY (id, username)
ORDER BY (id, username, sourcedate)
SETTINGS index_granularity = 8192;

CREATE TABLE sgk.tmp_urluserpass
(
	`id` Int64,
    `username` String,
    `password` String,
    `url` String,
    `source` String,
    `sourcedate` Date,
    INDEX idx_username_password_url (username, password, url) TYPE minmax GRANULARITY 8192,
    INDEX idx_username (username) TYPE minmax GRANULARITY 8192,
    INDEX idx_url (url) TYPE minmax GRANULARITY 8192
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sourcedate)  -- 以月份为单位进行分区
PRIMARY KEY (id, username)
ORDER BY (id, username, sourcedate)
SETTINGS index_granularity = 8192;
```

配合操作
```
go run import/parseimport.go /mnt/e/data/xxx/2024_08 2024-08-01;


go run dedup/deduplication.go;
运行成功之后，可以先count或者查看下数据，如果要替换（慎重）：
truncate urluserpass;     
go run import/parseimport.go /mnt/e/tgdata/Cloud\ REDhat/2024_08 2024-08-01;
```




## 附录
一些奇葩的格式
```
//accounts.google.com:|:gilson2lopes@gmail.com:|:Sa03252804#:|:https
https://zayiraldeenu@gmail.com:Ah1236789:Application::Google_[Chrome]_Profile:6
https://accounts.google.com/signin/v2/challenge/pwd:mommyhacker1s2sk@gmail.com:Google_[Chrome]_Default
https://wszy%+*w_*7YSD#123:Application::Google_[Chrome]_Default:===============
https://vktarget.ru/list:amid53532@gmail.com::t1158lNP:Mail.Ru_[Atom]_Default
https://accounts.google.com/SignUp;ong.makhtoutat:tichit123
http://ajaymachine 8083/saggst/userhome.htm Login::avijit_saha2017:Agartala@121
http://localhost 8083/gengst/gst/client_clientlist Login::avijit_saha2017:Agartala@121
https://redfin.id.rapidplex.com:2083|Domains 2|Listsch.legalitas.site,legalitas.site legalit6::YV00xfcK5Yc6#
https://ruangguree.com 2083 | Domains: 8 | List akademiaudit.or.id, chressindogroup.com, phria.or.id, fkmonline-usm.com, coaching.ruangguree.com, codesk.ruangguree.com, instruktur.ruangguree.com, ruangguree.com ruae1843:Erwin212077
```






