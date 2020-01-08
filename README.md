# emqx_restart_resume
> 用于emqx开源版 服务重启后恢复原订阅主题和持久化数据

## 问题：

 1. 开源版emq在服务重启后原订阅的主题会清空，在客户端保持原clientId，保持原session未重新订阅时，接不到服务器转发的消息。
 2. 开源版持久化会模型保存主题下的最后一条消息，在重启后也会被清空。

## 解决方案：
利用EMQ X Web Hook插件将时间发送到指定的请求，利用Redis 和 EMQ X自带的Web API进行扩展，可查看博文[了解详情](https://blog.csdn.net/ctwy291314/article/details/103820919)


## 相关依赖
- python 3
- pip install redis    
- pip install paho-mqtt
- pip install Flask

> 说明：利用redis进行数据持久化，利用Flask搭建Web服务，利用paho-mqtt创建mqtt客户端并检查连接状态

## TODO
- 利用emqx 的插件模版，使用Erlang编程语言实现上述流程 



