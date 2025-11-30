# VeighNa框架的IoTDB数据库接口

## 参考自 https://github.com/shizy818/vnpy-iotdb

## 注意，本仓库只能适用于我自己修改的vnpy，因为使用了一些我加入新功能，所以不能使用公版vnpy  

# 数据库结构
root.vnpy.interval.exchange.symbol  
root.vnpy.overview.interval.exchange.symbol  

第三层为子数据库，第二层 vnpy 是根文件夹  
同时第三层挂载 device，并且使用了不同的时间分区参数，可以减少大量元数据的内存占用，同时能加速统计

| 子数据库   |时间分区参数|
|----------|----|
| tick     |1个月|
| bar_1m   |3个月|
| bar_5m   |6个月|
| bar_1d   |1年|
| bar_dr   |10年|
| overview |10年|


## 说明
基于IoTDB的[Python原生接口](https://iotdb.apache.org/zh/UserGuide/latest/API/Programming-Python-Native-API.html)开发的IoTDB数据库接口。  

**需要使用IoTDB 2.0.x版本的树模型。**

## 使用

在VeighNa中使用时，需要在全局配置中填写以下字段信息：  

|名称|含义|必填| 举例        |
|---------|----|---|-----------|
|database.name|名称|是| iotdb     |
|database.host|地址|是| localhost |
|database.port|端口|是| 6667      |
|database.database|实例|是| vnpy      |
|database.user|用户名|是| root      |
|database.password|密码|是| root      |

### 连接
连接前需要根据环境安装配置IoTDB服务端，IoTDB的安装流程请参考[官方文档](https://iotdb.apache.org/zh/UserGuide/latest/QuickStart/QuickStart_apache.html)。

# MIT协议
