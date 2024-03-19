# QUANTFLY
量化框架
## 踩坑记录
1. Postgresql windows 版本编码问题
2. 
插入时会显示"UnicodeDecodeError: 'utf-8' codec can't decode byte 0xd6 in position 61: invalid continuation byte "
是客户端与服务器端编码方式不同 服务器端为utf8 本地客户端为GBK(汉语计算机会是这个)，所以导致连接不上。解决办法：
https://stackoverflow.com/questions/20952893/postgresql-encoding-problems-on-windows-when-using-psql-command-line-utility
open the cmd
SET PGCLIENTENCODING=utf-8
chcp 65001
psql -h your.ip.addr.ess -U postgres

2. 