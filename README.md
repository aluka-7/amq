# AMQ定义
AMQ引擎定义，允许各系统之间通过AMQ进行消息异步交互，默认情况下系统未开启AMQ功能，如果本系统有需要使用需要在配置
文件 ` /base/amq `中进行如下配置方可开启:
```
 {
    "enabled":"true", //是否开启AMQ服务
}
```
同时，业务系统对每个AMQ节点需要注册一个或多个{@link Processor}消息处理器，否则收到的消息由于无法找到对应的消息处理器而会被忽略，从而对业务造成影响，业务系统需要实现上述接口并在启动{@link Client}之前手动注册消息处理器到对应的客户端中。

另外，对于不同的AMQ节点需要在配置中心进行如下的配置，配置的key为：
```
/system/base/amq/{node}
```
其中{node}为节点的唯一标示，其数据格式可参看{@link AMQClient}的配置说明。

# Client定义

提供给业务系统使用和AMQ进行交互的接口，允许业务系统发送消息到AMQ和处理从AMQ中收到的消息。每个AMQ客户端都需要指定一个唯一的标示以及初始化AMQ客户端所需要的配置参数(config)，配置参数的格式如下：
```
 {
   "provider" : "rabbit", // AMQ的提供器
   "parameter" : {          // AMQ提供器的初始化参数，不同AMQ提供所需要的初始化参数不一样
     "key1" : "value1",
     "key2" : "value2"
   },
   "partitions" : 2         // 节点的分区数(默认1，可选配置)
 }
```
需要特别注意的是，每个系统实例化一个{@link Client}后，该实例会唯一的只监听使用该系统ID标示的一个队列，而这个队列的名称格式为：
 ```
 sys_amq_{systemId}_{node}
```
其中{systemId}即为当前系统的唯一四位数数字标示，而{node}则标明该客户端连接到的AMQ节点。

获取当前系统对当前节点的分区配置(可选)，如果配置了则只监听指定的分区，需要在/system/base/amq/{systemId}中按照如下格式配置：
```
 client.Start([]int{1,2,3})
```
# 温馨提示：
获取目标系统的队列名称可使用方法 `client.BuildQueueName(systemId)`来获取
