package esb

import (
	"fmt"

	"git.forchange.cn/base/esb/message"
	"git.forchange.cn/base/esb/provider"
)

/**
 * 处理收到的新消息（包括通知消息和事务消息）。
 *
 * @param message
 * @param listener
 * @throws ESBException
 */
func HandleNew(msg *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	if msg.Category == message.NOTICE {
		return noticeNew(msg, listener)
	} else if msg.Category == message.SIMPLEX {
		return simplexNew(msg, listener)
	} else if msg.Category == message.DUPLEX {
		return duplexNew(msg, listener)
	} else {
		return nil, fmt.Errorf("无效的消息类型:%d", msg.Category)
	}
}

/**
 * 处理收到的单向/双向事务消息的应答消息。
 *
 * @param message
 * @param listener
 * @throws ESBException
 */
func HandleAck(msg *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	if msg.Category == message.SIMPLEX {
		phase := msg.Phase
		if phase == message.ReceiverAck {
			return simplexRecipientACK(msg, listener)
		} else {
			return nil, fmt.Errorf("无效的消息阶段：%d", msg.Phase)
		}
	} else if msg.Category == message.DUPLEX {
		phase := msg.Phase
		if phase == message.ReceiverAck {
			return duplexRecipientACK(msg, listener)
		} else if phase == message.SenderAck {
			return duplexSenderACK(msg, listener)
		} else {

			return nil, fmt.Errorf("无效的消息阶段：%d", msg.Phase)
		}
	} else {
		return nil, fmt.Errorf("无效的消息类型:%d", msg.Category)
	}
}
func noticeNew(msg *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	phase := msg.Phase
	if phase != message.SenderReq {
		return nil, fmt.Errorf("无效的消息阶段：%d", msg.Phase)
	}
	nm, err := msg.ConvertToNotice()
	if err != nil {
		return nil, err
	}
	_, err = listener.OnReceived(nm)
	return nil, err
}
func simplexNew(mpl *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	phase := mpl.Phase
	if phase != message.SenderReq {
		return nil, fmt.Errorf("无效的消息阶段:%d", phase)
	}
	m, err := mpl.ConvertToSimplex()
	if err != nil {
		return nil, err
	}
	// 单向事务新消息送达，接收方处理并应答
	rsp, err := listener.OnReceived(m)
	if rsp == nil {
		return nil, err
	}
	returnMsg := message.NewPayload(mpl, message.ReceiverAck)
	returnMsg.SetBody(rsp)
	returnMsg.SetSign(message.Signature(returnMsg))
	return returnMsg, err
}
func duplexNew(mpl *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	phase := mpl.Phase
	if phase != message.SenderReq {
		return nil, fmt.Errorf("无效的消息阶段:%d", phase)
	}
	m, err := mpl.ConvertToDuplex()
	if err != nil {
		return nil, err
	}
	// 双向事务新消息送达，接收方处理并应答
	rsp, err := listener.OnReceived(m)
	if rsp == nil {
		return nil, err
	}
	returnMsg := message.NewPayload(mpl, message.ReceiverAck)
	returnMsg.SetBody(rsp)
	returnMsg.SetSign(message.Signature(returnMsg))
	return returnMsg, err
}

func simplexRecipientACK(mpl *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	phase := mpl.Phase
	if phase != message.ReceiverAck {
		return nil, fmt.Errorf("无效的消息阶段：%d", mpl.Phase)
	}
	// 单向事务应答消息，发送方处理
	_, err := listener.OnRecipientAckReceived(mpl.Genre, mpl.MsgId, mpl.Body)
	return nil, err
}
func duplexRecipientACK(mpl *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	phase := mpl.Phase
	if phase != message.ReceiverAck {
		return nil, fmt.Errorf("无效的消息阶段：%d", mpl.Phase)
	}
	// 双向事务的接收方应答消息送达，发送方处理并进行应答
	rsp, err := listener.OnRecipientAckReceived(mpl.Genre, mpl.MsgId, mpl.Body)
	if rsp == nil {
		return nil, err
	}
	returnMsg := message.NewPayload(mpl, message.SenderAck)
	returnMsg.SetBody(rsp)
	returnMsg.SetSign(message.Signature(returnMsg))
	return returnMsg, err
}
func duplexSenderACK(mpl *message.MsgPayload, listener provider.MessageListener) (*message.MsgPayload, error) {
	phase := mpl.Phase
	if phase != message.SenderAck {
		return nil, fmt.Errorf("无效的消息阶段：%d", mpl.Phase)
	}
	// 双向事务的发送方应答消息送达，接收方处理
	err := listener.OnSenderAckReceived(mpl.Genre, mpl.MsgId, mpl.Body)
	return nil, err
}
