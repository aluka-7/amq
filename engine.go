package amq

import (
	"fmt"

	"github.com/aluka-7/amq/node"
	"github.com/aluka-7/configuration"
)

type Config struct {
	conf configuration.Configuration
}

/**
 * 获取企业消息总线(AMQ)管理引擎的唯一实例。
 * @param config
 * @return
 */
func Engine(conf configuration.Configuration, systemId string) (amq *Amq) {
	fmt.Println("Loading AMQ Engine ver:1.0.0")
	// 初始化所有的AMQ节点定义，用于后续的消息发送时的队列名称校验。
	amq = &Amq{
		conf:      conf,
		clientMap: make(map[node.Node]*Client),
		allNodes:  make([]node.Node, len(node.Values())),
		systemId:  systemId,
	}
	for i, v := range node.Values() {
		amq.allNodes[i] = v
	}
	return amq
}

/**
 * AMQ引擎定义，允许各系统之间通过AMQ进行消息异步交互，默认情况下系统未开启AMQ功能，如果本系统有需要使用需要在配置
 * 文件<b>/base/amq</b>中进行如下配置方可开启：
 * <pre>
 * amq.enabled=true
 * </pre>
 * <p>
 * 同时，业务系统对每个AMQ节点需要注册一个或多个{@link Processor}消息处理器，否则收到的消息由于无法找到对应的
 * 消息处理器而会被忽略，从而对业务造成影响，业务系统需要实现上述接口并在启动{@link Client}之前手动注册消息处理
 * 器到对应的客户端中。
 * </p>
 * <p>
 * 另外，对于不同的AMQ节点需要在配置中心进行如下的配置，配置的key为：<b>/system/base/amq/{node}</b>，其中{node}
 * 为节点的唯一标示，其数据格式可参看{@link AMQClient}的配置说明。
 * </p>
 */
type Amq struct {
	conf      configuration.Configuration
	systemId  string
	allNodes  []node.Node
	clientMap map[node.Node]*Client
}

/**
 * 判断当前支持的AMQ节点中是否包含了指定的节点。
 *
 * @param node
 * @return
 */
func (e *Amq) Clean() {
	for _, v := range e.clientMap {
		v.Close()
	}
}

/**
 * 根据给定的节点标示来初始化对应的AMQ客户端实例，如果未开启AMQ功能或不存在该节点则会抛出异常，否则返回初始化后的
 * AMQ客户端实例，该方法同时会缓存已经初始化好的实例，所以多次使用同一个节点标示返回的实例都是同一个。
 *
 * @param node 可参看{@link Node}的说明
 * @return
 */

// 配置示例：create /system/base/amq/biz
func (e *Amq) Client(node node.Node) (*Client, error) {
	if node.IsValid() != nil {
		return nil, fmt.Errorf("无效的AMQ节点[%d]", node)
	}
	client, ok := e.clientMap[node]
	if !ok {
		client = newClient(e.conf, e.systemId, node)
		e.clientMap[node] = client
	}
	return client, nil
}
