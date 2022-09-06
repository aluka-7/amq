package provider

import (
	"sync"

	"github.com/aluka-7/amq/message"
	"github.com/aluka-7/amq/node"
)

/**
 * 企业服务总线（ESB）的客户端接口，通过ESB实现系统解耦并实现系统之间的交互需求，目前支持的系统交互有如下几类：
 *
 * 消息通知：发送方单纯的发出通知给接收方，不对接收方的处理结果进行响应；
 * 单向事务：发送方发出请求给接收方，等待接收方反馈处理结果（成功或失败）后进行处理；
 * 双向事务：发送方发出请求给接收方，等待接收方处理结果后，再次通知接收方我方的处理结果；
 *
 * 基于上述交互需求，所选择的ESB中间件要满足如下条件：
 *
 * 消息的顺序性：发送方发出的消息顺序和接收方接收到的消息顺序要一致；
 * 唯一消费保证：同一个消息队列中的消息只能被一个消费者消费；
 * 消息通知保证：中间件要保证消息正确无误的送达给消费者并且被成功消费；
 */
var (
	providersMu sync.RWMutex
	providers   = make(map[string]Provider)
)

func Register(name string, provider Provider) {
	providersMu.Lock()
	defer providersMu.Unlock()
	if provider == nil {
		panic("esb: Register provider is nil")
	}
	if _, dup := providers[name]; dup {
		panic("esb: Register called twice for provider " + name)
	}
	providers[name] = provider
}
func Read(key string) Provider {
	p := providers[key]
	return p
}

type Provider interface {
	// 初始化客户端接口
	New(node node.Node, cfg map[string]string) Provider
	/**
	 * 监听指定的消息队列，当有新消息或事务应答消息送达时会调用监听器接口，可同时监听多个不同的队列。特别注意：同一个消息队列只能有一个监听器！
	 *
	 * @param name     要监听的消息队列
	 * @param listener 消息监听器
	 * @return closer: 关闭监听动作
	 * @throws error
	 */
	Listen(name string, listener MessageListener) (closer func(), err error)

	/**
	 * 取消对指定队列的监听。
	 *
	 * @param name
	 */
	Cancel(name string)

	/**
	 * 发送ESB消息，可根据业务需求发送三类消息：
	 * <ul>
	 * <li>{@link ESBNoticeMessage}：通知类消息，只保证消息被成功发送到ESB中。</li>
	 * <li>{@link ESBSimplexMessage}：单向事务消息，通过收到接收方的确认消息来完成事务。</li>
	 * <li>{@link ESBDuplexMessage}：双向事务消息，通过接收方和发送方各分别确认来完对应的事务。</li>
	 * </ul>
	 *
	 * @param message
	 * @throws error
	 */
	Send(message interface{}) error

	/**
	 * 关闭到消息中间件的连接，清除资源。
	 */
	Close()
}

/**
 * ESB消息的监听器接口定义。
 */
type MessageListener interface {
	/**
	 * 接收消息并进行处理，如果处理失败则抛出异常。如果需要返回消息（如事务消息需要返回）请返回消息体内容，否则请返回Null。
	 *
	 * @param message
	 * @return
	 * @throws ESBException
	 */
	OnReceived(msg interface{}) (*message.MsgBody, error)
	/**
	 * 处理接收方发出的ACK消息，由消息发送方处理。在双向事务消息中消息发送方还应该在处理完成后返回一个应答消息通知接收方处理结果。
	 * 如果返回结果为Null则不发送。
	 *
	 * @param genre  消息类型
	 * @param msgId 事务消息的唯一ID
	 * @param rsp   接收方返回的应答消息
	 * @return
	 * @throws ESBException
	 */
	OnRecipientAckReceived(genre, msgId string, rsp *message.MsgBody) (*message.MsgBody, error)

	/**
	 * 处理发送方发出的ACK消息，由接收方处理，该方法仅在双向事务消息中有效，通知接收方最终发送方的处理结果。
	 *
	 * @param type  消息类型
	 * @param msgId 事务消息的唯一ID
	 * @param rsp   发送方返回的应答消息
	 * @throws ESBException
	 */
	OnSenderAckReceived(genre, msgId string, rsp *message.MsgBody) error
}
