# 说明
数据库结构，树结构模式

root.vnpy.bar.interval.exchange.symbol
root.vnpy.tick.exchange.symbol
root.vnpy.dr.exchange.symbol

overview 放在 symbol 节点中
其中 root.vnpy 为数据库根节点

时间分区为1个月
优点，维护比较方便
缺点，元数据很多，很占内存，生成overview的统计也很慢
