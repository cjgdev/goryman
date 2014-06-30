package goryman

import (
	"fmt"
	"net"
	"sync"

	"github.com/bigdatadev/goryman/proto"
)

type GorymanClient struct {
	sync.Mutex
	udp  net.Conn
	tcp  net.Conn
	addr string
}

func NewGorymanClient(addr string) *GorymanClient {
	return &GorymanClient{
		addr: addr,
	}
}

func (c *GorymanClient) Connect() error {
	udp, err := net.DialTimeout("udp", c.addr, time.Second)
	if err != nil {
		return err
	}
	tcp, err := net.DialTimeout("tcp", c.addr, time.Second)
	if err != nil {
		return err
	}
	c.udp = udp
	c.tcp = tcp
	return nil
}

func (c *GorymanClient) Close() error {
	c.Lock()
	defer c.Unlock()
	if nil == c.udp && nil == c.tcp {
		return nil
	}
	err := c.udp.Close()
	if err != nil {
		return err
	}
	return c.tcp.Close()
}

func (c *GorymanClient) SendEvent(e *Event) error {
	return SendEventTransport(e, "udp")
}

func (c *GorymanClient) SendEventTransport(e *Event, t string) error {
	epb, err := EventToProtocolBuffer(e)
	if err != nil {
		return err
	}

	message := &proto.Msg{}
	message.Events = append(message.Events, epb)

	return SendMessageTransport(message, t)
}

func (c *GorymanClient) SendState(s *State) error {
	return SendEventTransport(s, "udp")
}

func (c *GorymanClient) SendStateTransport(s *State, t string) error {
	spb, err := StateToProtocolBuffer(s)
	if err != nil {
		return err
	}

	message := &proto.Msg{}
	message.States = append(message.States, spb)

	return SendMessageTransport(message, t)
}

func (c *GorymanClient) QueryEvents(q string) ([]Event, error) {
	c.Lock()
	defer c.Unlock()

	query := &proto.Query{}
	query.String_ = pb.String(q)

	message := &proto.Msg{}
	message.Query = query

	response, err := SendTcp(message, c.tcp)
	if err != nil {
		return nil, err
	}

	return ProtocolBuffersToEvents(response.GetEvents()), nil
}

func (c *GorymanClient) SendMessage(m *Message) error {
	return SendMessageTransport(m, "udp")
}

func (c *GorymanClient) SendMessageTransport(m *Message, t string) error {
	c.Lock()
	defer c.Unlock()

	switch t {
	case "udp":
		_, err := SendUdp(message, c.udp)
		if err != nil {
			SendTcp(message, c.tcp)
		}
	case "tcp":
		SendTcp(message, c.tcp)
	default:
		return fmt.Errorf("cannot send message, unknown transport")
	}

	return nil
}
