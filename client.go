package amq

import (
	"fmt"
	"regexp"

	"github.com/aluka-7/amq/message"
	"github.com/aluka-7/amq/node"
	"github.com/aluka-7/amq/provider"
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/utils"
	"github.com/rs/zerolog/log"
) /**
 * 提供给业务系统使用和AMQ进行交互的接口，允许业务系统发送消息到AMQ和处理从AMQ中收到的消息。每个AMQ客户端
 * 都需要指定一个唯一的标示以及初始化AMQ客户端所需要的配置参数(config)，配置参数的格式如下：
 * <pre>
 * {
 *   "provider" : "Rabbit", // AMQ的提供器
 *   "parameter" : {          // AMQ提供器的初始化参数，不同AMQ提供所需要的初始化参数不一样
 *     "key1" : "value1",
 *     "key2" : "value2"
 *   },
 *   "partitions" : 2         // 节点的分区数(默认1，可选配置)
 * }
 * </pre>
 * <p>
 * 需要特别注意的是，每个系统实例化一个{@link Client}后，该实例会唯一的只监听使用该系统ID标示的一个队列，
 * 而这个队列的名称格式为：<b>sys_amq_{systemId}_{node}</b>，其中{systemId}即为当前系统的唯一四位数数字标示，
 * 而{node}则标明该客户端连接到的AMQ节点。
 * 获取当前系统对当前节点的分区配置(可选)，如果配置了则只监听指定的分区，需要在/system/base/amq/{node}中按照如下格式配置：
 * <pre>
 * {"partitions":"1,2,3"}
 * </pre>
 * </p>
 * <p>
 * <font color="red">温馨提示</font>：获取目标系统的队列名称可使用方法 {@link #buildQueueName(long, int)} 来获取
 * </p>
 */

type Client struct {
	systemId         string
	conf             configuration.Configuration
	node             node.Node
	queueNamePattern *regexp.Regexp
	partitions       int
	provider         provider.Provider
	processorMap     map[string]Processor
	started          bool
}

type ClientConfig struct {
	Provider   string            `json:"provider"`
	Parameter  map[string]string `json:"parameter"`
	Partitions int               `json:"partitions"` // 分区数量
}

/**
 * 给定amq节点的唯一标示和连接到amq的配置来构造一个amq客户端，如果初始化失败则抛出异常。该初始化方法会
 * 同步初始化对应节点的所有消息处理器并启动监听。
 *
 * @param node
 * @param config
 */
// {"provider":"Rabbit","parameter":{"username":"guest","password":"guest","brokerURL":"localhost:5672"},"partitions":1}
func newClient(conf configuration.Configuration, systemId string, node node.Node) *Client {
	client := &Client{conf: conf, node: node, systemId: systemId}
	cfg := &ClientConfig{}
	err := conf.Clazz("base", "amq", "", node.String(), cfg)
	if err != nil {
		log.Fatal().Msg("fetch amq config error")
	}

	client.partitions = cfg.Partitions

	if client.partitions == 1 {
		client.queueNamePattern, _ = regexp.Compile("(sys_amq_\\d{4})_(.+)")
	} else {
		client.queueNamePattern, _ = regexp.Compile("(sys_amq_\\d{4})_(.+)_p\\d+")
	}

	if len(cfg.Provider) > 0 {
		read := provider.Read(cfg.Provider)
		if read == nil {
			log.Fatal().Msgf("[AMQ-Client-%s]不支持的provider类型:%s", node.String(), cfg.Provider)
			return nil
		}
		client.provider = read.New(node, cfg.Parameter)
	}

	client.processorMap = make(map[string]Processor, 0)
	fmt.Printf("[AMQ-Client-%s]客户端初始化完成:config=%v\n", node.String(), cfg)
	return client
}

/**
 * 为当前客户端添加一个或多个消息处理器，需要确保该方法在{@link #start()}方法之前调用，否则系统会抛出异常。
 *
 * @param processors
 */
func (c *Client) AddProcessor(processors ...Processor) {
	if !c.started {
		if len(processors) > 0 {
			for _, p := range processors {
				c.processorMap[p.GetType()] = p
			}
		}
	} else {
		fmt.Printf("[AMQ-Client-%s]该客户端已启动，无法添加消息处理器\n", c.node.String())
	}
}

/**
 * 使用当前客户端构建一个amq消息的目标队列名称，目标队列名称满足格式：sys_amq_{systemId}_{node}，
 * 其中{systemId}为目标系统的四位数数字ID，{node}为目标系统监听的amq节点标示(参考{@link AMQNode}。
 *
 * @param destSystemId 目标系统ID
 * @param destNode     目标AMQ节点
 * @return
 */
func (c *Client) BuildQueueName(systemId string) string {
	if c.partitions > 1 {
		log.Info().Msg("多分区节点请构建分区的队列名称")
	}
	return fmt.Sprintf("sys_amq_%s_%s", systemId, c.node.String())
}

/**
 * 等同{@link #buildQueueName(string)}方法，唯一不同的是如果节点配置为了多分区则需要指定分区编号
 * 来依次构建每个分区的队列，分区编号从0开始。
 *
 * @param destSystemId
 * @param destNode
 * @param partition
 * @return
 */
func (c *Client) BuildQueueNameByPartition(systemId string, partition int) string {
	if c.partitions > 1 {
		log.Error().Msg("单分区节点请构建单分区的队列名称")
	}
	if partition >= 0 && partition < c.partitions {
		log.Error().Msg("分区编号指定错误")
	}
	return fmt.Sprintf("sys_amq_%s_%s_p%d", systemId, c.node.String(), partition)
}

/**
 * 在当前节点上启动进行本地系统的队列监听，该方法请务必在{@link #AddProcessor(...Processor)}方法之后调用，否则可能会导致部分消息
 * 由于没有对应的消息处理器而丢失。<font color="red">特别注意：如果收到的消息类型没有对应的消息处理器，系统只会简单的丢弃并打印告警信息！</font>
 * 同时注意，该方法只能被调用一次，如果多次调用则后续的调用会抛出异常。
 *
 * @throws error
 */
func (c *Client) Start(partitions []int) (closer func(), err error) {
	// 只能被启动一次
	if c.started {
		return nil, fmt.Errorf("[AMQ-Client-%s]该客户端已启动，无法多次启动", c.node.String())
	}
	c.started = true
	// 获取当前系统对当前节点的分区配置(可选)，如果配置了则只监听指定的分区，需要在/system/base/amq/{systemId}中按照如下格式配置:{"partitions":"1,2,3"}
	for _, v := range partitions {
		if v == 0 {
			continue
		}
		if v <= 0 && c.partitions > v {
			panic("本地节点监听的分区编号错误：" + utils.ToStr(v))
		}
	}
	listener := &defaultMessageListener{
		processor: func(genre string) Processor {
			processor := c.processorMap[genre]
			if processor == nil {
				log.Error().Msgf("[AMQ-Client-%s]未定义消息类型的处理器:%s", c.node.String(), genre)
			}
			return processor
		},
		node: c.node,
	}

	// 监听当前系统在AMQ节点上的队列，如果有分区则按照分区分队列控制，另外，如果本地配置了启动分区编号则只监听指定的分区队列
	if c.partitions == 1 {
		queueName := c.BuildQueueName(c.systemId)
		log.Info().Msgf("[AMQ-Client-%s]启动监听AMQ单分区消息队列:queue=%s", c.node.String(), queueName)
		closer, err = c.provider.Listen(queueName, listener)
	} else {
		if len(partitions) == 0 {
			for i := 0; i < c.partitions; i++ {
				queueName := c.BuildQueueNameByPartition(c.systemId, i)
				log.Info().Msgf("[AMQ-Client-%s]启动监听AMQ多分区消息队列:partition=%d,queue=%s", c.node.String(), i, queueName)
				closer, err = c.provider.Listen(queueName, listener)
			}
		} else {
			for _, v := range partitions {
				queueName := c.BuildQueueNameByPartition(c.systemId, v)
				log.Info().Msgf("[AMQ-Client-%s]启动监听AMQ多分区消息队列:partition=%d,queue=%s", c.node.String(), v, queueName)
				closer, err = c.provider.Listen(queueName, listener)
			}
		}
	}
	return
}

/**
 * 判断当前节点是否支持多分区。
 *
 * @return
 */
func (c *Client) IsMultiplePartition() bool {
	return c.partitions > 1
}

/**
 * message 的格式检查
 * @param message
 * @return message, err
 */

func (c *Client) messageCheck(msg interface{}) (interface{}, error) {
	nameList := make([]string, 0)
	switch msg.(type) {
	case *message.NoticeMessage:
		msg := msg.(*message.NoticeMessage)
		nameList = append(nameList, msg.Destination)
	case *message.SimplexMessage:
		msg := msg.(*message.SimplexMessage)
		nameList = append(nameList, msg.Source)
		nameList = append(nameList, msg.Destination)
	case *message.DuplexMessage:
		msg := msg.(*message.DuplexMessage)
		nameList = append(nameList, msg.Source)
		nameList = append(nameList, msg.DestinationNew)
		nameList = append(nameList, msg.DestinationAck)
	}
	// 检查名称规范
	for _, name := range nameList {
		m := c.queueNamePattern.FindStringSubmatch(name)
		if len(m) == 0 {
			return nil, fmt.Errorf("AMQ消息队列名称不符合规范:%s", name)
		} else {
			nodeName := m[2]
			if node.GetNode(nodeName).IsValid() != nil {
				log.Error().Msgf("AMQ消息队列节点错误:%s", nodeName)
			}
		}
	}
	return msg, nil
}

/**
 * 发送新消息到AMQ中，这里是所有新消息的发送入口，如果发送失败则会抛出异常。请注意，消息的目标队列名称请使用
 * 方法 {@link #buildQueueName(long, String, int)} 来构建并设置，不满足格式的目标队列名称会导致消息发送失败。
 *
 * @param message
 * @throws AMQException
 */
func (c *Client) Send(msg interface{}) error {
	msg, err := c.messageCheck(msg)
	if err != nil {
		return err
	}
	// 发送消息
	if err = c.provider.Send(msg); err == nil {
		log.Debug().Msgf("[AMQ-Client-%s]消息发送成功:%+v", c.node.String(), msg)
	}
	return err
}

/**
 * 关闭所有的资源，该方法不会抛出任何异常。
 */
func (c *Client) Close() {
	c.provider.Close()
}

/**
 * AMQ消息的处理器接口定义，业务系统实现该接口后需要手动注册到{@link AMQClient}中去方可生效。
 */
type Processor interface {
	/**
	 * 获取要处理的消息类型，对应{@link AMQMessage}中的type字段。
	 *
	 * @return
	 */
	GetType() string

	/**
	 * 接收新消息并进行处理，如果新消息为事务消息并且需要返回处理结果则请返回处理结果，否则返回Null即可。
	 *
	 * @param message
	 * @throws AMQException
	 */
	OnReceived(msg interface{}) (*message.MsgBody, error)

	/**
	 * 单向/双向事务消息中的接收方应答消息的接收处理，对于单向事务消息处理完成后返回Null即可，对于双向事务消息则还需要
	 * 返回系统的处理结果给到接收方进行后续处理。
	 *
	 * @param msgId
	 * @param rsp
	 * @return
	 */
	OnRecipientAckReceived(msgId string, rsp *message.MsgBody) (*message.MsgBody, error)

	/**
	 * 双向事务消息中的发送方的确认应答消息，由接收方进行处理。
	 *
	 * @param msgId 事务消息的唯一ID
	 * @param rsp   发送方返回的应答消息
	 */
	OnSenderAckReceived(msgId string, rsp *message.MsgBody) error
}

type defaultMessageListener struct {
	node      node.Node
	processor func(genre string) Processor
}

func (l *defaultMessageListener) OnReceived(msg interface{}) (*message.MsgBody, error) {
	log.Debug().Msgf("[AMQ-Client-%s]收到新消息:%v", l.node.String(), msg)
	processor := l.processor(message.GetGenre(msg))
	if processor != nil {
		return processor.OnReceived(msg)
	} else {
		return nil, fmt.Errorf("此类型AMQ消息的处理器接口定义:无")
	}
}

func (l *defaultMessageListener) OnRecipientAckReceived(genre, msgId string, rsp *message.MsgBody) (*message.MsgBody, error) {
	log.Debug().Msgf("[AMQ-Client-%s]收到接收方应答消息：type=%s,msgId=%s,rsp=%v", l.node.String(), genre, msgId, rsp)
	processor := l.processor(genre)
	if processor != nil {
		return processor.OnRecipientAckReceived(msgId, rsp)
	} else {
		return nil, fmt.Errorf("此类型AMQ消息的处理器接口定义:无")
	}
}
func (l *defaultMessageListener) OnSenderAckReceived(genre, msgId string, rsp *message.MsgBody) error {
	log.Debug().Msgf("[AMQ-Client-%s]收到发送方应答消息:type=%s,msgId=%s,rsp=%v", l.node.String(), genre, msgId, rsp)
	processor := l.processor(genre)
	if processor != nil {
		return processor.OnSenderAckReceived(msgId, rsp)
	} else {
		return nil
	}
}
