package message

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/aluka-7/utils"
)

/**
 * ESB消息的类型。
 */
type _MessageCategory string

func (n _MessageCategory) String() string {
	mc := map[_MessageCategory]string{
		"1": "NOTICE",
		"2": "SIMPLEX",
		"3": "DUPLEX",
	}
	if v, ok := mc[n]; ok {
		return v
	} else {
		return "Unknown"
	}
}

const (
	// 通知消息
	NOTICE _MessageCategory = "1"
	// 单向事务消息
	SIMPLEX _MessageCategory = "2"
	// 双向事务消息
	DUPLEX _MessageCategory = "3"
)

/**
 * ESB事务消息的阶段定义。
 *
 */
type _MessagePhase string

func (n _MessagePhase) String() string {
	mc := map[_MessagePhase]string{
		"1": "SENDER_REQ",
		"2": "RECEIVER_ACK",
		"3": "SENDER_ACK",
	}
	if v, ok := mc[n]; ok {
		return v
	} else {
		return "Unknown"
	}
}

const (
	// 消息已发出，发送方已发出消息，所有新消息的初始状态
	SenderReq _MessagePhase = "1"
	// 接收方已应答，事务消息中的应答阶段
	ReceiverAck _MessagePhase = "2"
	// 发送方已应答，对应双向事务消息中的发送方应答
	SenderAck _MessagePhase = "3"
)

/**
 * 每条ESB消息的唯一ID标示对象。
 */
type msgId struct {
	msgId string
}

/**
 * 创建一个新的MsgId实例并返回。
 */
func NewMsgId() *msgId {
	return &msgId{
		msgId: utils.ToStr(time.Now().UnixNano()),
	}
}

/**
 * 获取该消息的唯一标示字符串。
 */
func (mid msgId) Id() string {
	return mid.msgId
}

/**
 * ESB消息体封装，提供流式操作。
 */
type MsgBody struct {
	Body map[string]string `json:"body"`
}

func NewMessageBody() *MsgBody {
	return &MsgBody{
		Body: make(map[string]string, 0),
	}
}
func (mb *MsgBody) Add(key string, value interface{}) *MsgBody {
	if len(mb.Body) < 1 {
		mb.Body = make(map[string]string, 1)
	}
	mb.Body[key] = utils.ToStr(value)
	return mb
}

func (mb *MsgBody) HasKey(key string) bool {
	_, ok := mb.Body[key]
	return ok
}

func (mb *MsgBody) Get(key string) string {
	return mb.Body[key]
}

func (mb *MsgBody) GetInt(key string) int {
	return utils.StrTo(mb.Body[key]).MustInt()
}

func (mb *MsgBody) GetInt64(key string) int64 {
	return utils.StrTo(mb.Body[key]).MustInt64()
}

func (mb *MsgBody) GetFloat(key string) float64 {
	return utils.StrTo(mb.Body[key]).Float64()
}

/**
输出ESB消息体内容
*/
func (mb *MsgBody) ToString() string {
	size := len(mb.Body)
	if size == 0 {
		return ""
	}
	keys := make([]string, 0, size)
	for k, _ := range mb.Body {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString("\"")
		buffer.WriteString(k)
		buffer.WriteString("\":\"")
		buffer.WriteString(mb.Body[k])
		buffer.WriteString("\"")
	}
	buffer.WriteString("}")
	return buffer.String()
}

/**
 * 所有ESB消息的基类，业务系统根据情况可选择发送如下几类消息：
 * <ul>
 * <li>{@link NoticeMessage}：通知类消息；</li>
 * <li>{@link SimplexMessage}：单向事务消息；</li>
 * <li>{@link DuplexMessage}：双向事务消息；</li>
 * </ul>

 */
type Message struct {
	genre string
	msgId string
	Body  *MsgBody
}

func NewMessage(mid *msgId) *Message {
	m := new(Message)
	m.msgId = mid.msgId
	return m
}
func GetGenre(msg interface{}) string {
	switch msg.(type) {
	case *NoticeMessage:
		return msg.(*NoticeMessage).genre
	case *SimplexMessage:
		return msg.(*SimplexMessage).genre
	case *DuplexMessage:
		return msg.(*DuplexMessage).genre
	}
	return ""
}

func (m *Message) SetType(genre string) {
	m.genre = genre
}

/**
 * 设置消息的消息体
 *
 * @param Body
 */
func (m *Message) SetBody(body *MsgBody) {
	m.Body = body
}

/**
 * 发送到ESB的通知类消息，通知类消息只保证被正确投递到ESB队列中，不保证接收方是否处理成功。
 *
 */
type NoticeMessage struct {
	Message
	Destination string
}

func NewNoticeMessage(msgId string) *NoticeMessage {
	nm := new(NoticeMessage)
	nm.msgId = msgId
	return nm
}

type SimplexMessage struct {
	Message
	Source      string
	Destination string
}

func NewSimplexMessage(msgId string) *SimplexMessage {
	sm := new(SimplexMessage)
	sm.msgId = msgId
	return sm
}

type DuplexMessage struct {
	Message
	// 发送方应答队列
	Source string
	// 接受方请求队列
	DestinationNew string
	// 接收方应答队列
	DestinationAck string
}

func NewDuplexMessage(msgId string) *DuplexMessage {
	dm := new(DuplexMessage)
	dm.msgId = msgId
	return dm
}

/**
 * 发送到ESB中去的消息的封装，对通知消息和事务消息进行统一封装。
 *
 */
type MsgPayload struct {
	Category    _MessageCategory `json:"category"`    // 消息分类
	Genre       string           `json:"type"`        // 消息类型
	MsgId       string           `json:"msgId"`       // 消息的唯一ID，发送时自动生成
	SrcAckQueue string           `json:"srcAckQueue"` // 消息发送方的应答队列名称（对事务消息有效）
	DstNewQueue string           `json:"dstNewQueue"` // 消息接收方的新消息队列名称
	DstAckQueue string           `json:"dstAckQueue"` // 消息接收方的应答消息队列（对双向事务消息有效）
	Body        *MsgBody         `json:"body"`        // 业务数据
	SendTime    int64            `json:"sendTime"`    // 发送时间
	Phase       _MessagePhase    `json:"phase"`       // 消息所处的阶段
	Sign        string           `json:"sign"`        // 签名信息
}

func (mpl *MsgPayload) SetBody(body *MsgBody) {
	mpl.Body = body
}
func (mpl *MsgPayload) SetSign(sign string) {
	mpl.Sign = sign
}
func (mpl *MsgPayload) ConvertToNotice() (*NoticeMessage, error) {
	msg := NewNoticeMessage(mpl.MsgId)
	if mpl.Category != NOTICE {
		return msg, fmt.Errorf("非通知类ESB消息")
	}
	msg.genre = mpl.Genre
	msg.Body = mpl.Body
	msg.Destination = mpl.DstNewQueue
	return msg, nil
}
func (mpl *MsgPayload) ConvertToSimplex() (*SimplexMessage, error) {
	msg := NewSimplexMessage(mpl.MsgId)
	if mpl.Category != SIMPLEX {
		return msg, fmt.Errorf("非单向事务ESB消息")
	}
	msg.genre = mpl.Genre
	msg.Body = mpl.Body
	msg.Destination = mpl.DstNewQueue
	msg.Source = mpl.SrcAckQueue
	return msg, nil
}
func (mpl *MsgPayload) ConvertToDuplex() (*DuplexMessage, error) {
	msg := NewDuplexMessage(mpl.MsgId)
	if mpl.Category != DUPLEX {
		return msg, fmt.Errorf("非双向事务ESB消息")
	}
	msg.genre = mpl.Genre
	msg.Body = mpl.Body
	msg.DestinationNew = mpl.DstNewQueue
	msg.DestinationAck = mpl.DstAckQueue
	msg.Source = mpl.SrcAckQueue
	return msg, nil
}
func (mpl *MsgPayload) String() string {
	v, _ := json.Marshal(mpl)
	return string(v)
}

/**
 * 根据当前消息的类型和所处的阶段来决定发送的队列名称。
 *
 * @return
 */
func (mpl *MsgPayload) SendQueueName() (string, error) {
	if mpl.Phase == SenderReq {
		return mpl.DstNewQueue, nil
	} else if mpl.Phase == ReceiverAck {
		return mpl.SrcAckQueue, nil
	} else if mpl.Phase == SenderAck {
		return mpl.DstAckQueue, nil
	} else {
		return "", fmt.Errorf("无效的消息阶段:%s", mpl.Phase)
	}
}
func NewPayload(msg *MsgPayload, phase _MessagePhase) *MsgPayload {
	mpl := &MsgPayload{
		Category:    msg.Category,
		Genre:       msg.Genre,
		MsgId:       msg.MsgId,
		SrcAckQueue: msg.SrcAckQueue,
		DstNewQueue: msg.DstNewQueue,
		DstAckQueue: msg.DstAckQueue,
		SendTime:    time.Now().Unix(),
		Phase:       phase,
	}
	mpl.Body = msg.Body
	mpl.Sign = Signature(mpl)
	return mpl
}
func NoticePayload(message *NoticeMessage) *MsgPayload {
	mpl := &MsgPayload{
		Category:    NOTICE,
		Genre:       message.genre,
		MsgId:       message.msgId,
		DstNewQueue: message.Destination,
		SendTime:    time.Now().Unix(),
		Phase:       SenderReq,
	}
	mpl.Body = message.Body
	mpl.Sign = Signature(mpl)
	return mpl
}
func SimplexPayload(message *SimplexMessage) *MsgPayload {
	mpl := &MsgPayload{
		Category:    SIMPLEX,
		Genre:       message.genre,
		MsgId:       message.msgId,
		SrcAckQueue: message.Source,
		DstNewQueue: message.Destination,
		SendTime:    time.Now().Unix(),
		Phase:       SenderReq,
	}
	mpl.Body = message.Body
	mpl.Sign = Signature(mpl)
	return mpl
}
func DuplexPayload(message *DuplexMessage) *MsgPayload {
	mpl := &MsgPayload{
		Category:    DUPLEX,
		Genre:       message.genre,
		MsgId:       message.msgId,
		SrcAckQueue: message.Source,
		DstNewQueue: message.DestinationNew,
		DstAckQueue: message.DestinationAck,
		SendTime:    time.Now().Unix(),
		Phase:       SenderReq,
	}
	mpl.Body = message.Body
	mpl.Sign = Signature(mpl)
	return mpl
}
func Signature(mpl *MsgPayload) string {
	var buffer bytes.Buffer
	buffer.WriteString("category=")
	buffer.WriteString(mpl.Category.String())
	buffer.WriteString("type=")
	buffer.WriteString(mpl.Genre)
	buffer.WriteString("msgId=")
	buffer.WriteString(mpl.MsgId)
	buffer.WriteString("queue=")
	buffer.WriteString(mpl.SrcAckQueue)
	buffer.WriteString(mpl.DstAckQueue)
	buffer.WriteString(mpl.DstNewQueue)
	buffer.WriteString("body=")
	buffer.WriteString(mpl.Body.ToString())
	buffer.WriteString("@phase=")
	buffer.WriteString(mpl.Phase.String())
	buffer.WriteString("@sendTime=")
	buffer.WriteString(utils.ToStr(mpl.SendTime))
	buffer.WriteString("@#$dz874&*&*#@@$^&^FS()()!@FSF")
	return utils.MD5(buffer.String())
}
