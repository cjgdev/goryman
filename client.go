package goryman

import (
	"net"
	"time"

	pb "code.google.com/p/goprotobuf/proto"
	"github.com/bigdatadev/goryman/proto"
)

type GorymanClient struct {
	udp  *UdpTransport
	tcp  *TcpTransport
	addr string
}

func NewGorymanClient(addr string) *GorymanClient {
	return &GorymanClient{
		addr: addr,
	}
}

func (c *GorymanClient) Connect() error {
	udp, err := net.DialTimeout("udp", c.addr, time.Second*5)
	if err != nil {
		return err
	}
	tcp, err := net.DialTimeout("tcp", c.addr, time.Second*5)
	if err != nil {
		return err
	}
	c.udp = NewUdpTransport(udp)
	c.tcp = NewTcpTransport(tcp)
	return nil
}

func (c *GorymanClient) Close() error {
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
	epb, err := EventToProtocolBuffer(e)
	if err != nil {
		return err
	}

	message := &proto.Msg{}
	message.Events = append(message.Events, epb)

	_, err = c.sendMaybeRecv(message)
	return err
}

func (c *GorymanClient) SendState(s *State) error {
	spb, err := StateToProtocolBuffer(s)
	if err != nil {
		return err
	}

	message := &proto.Msg{}
	message.States = append(message.States, spb)

	_, err = c.sendMaybeRecv(message)
	return err
}

func (c *GorymanClient) QueryEvents(q string) ([]Event, error) {
	query := &proto.Query{}
	query.String_ = pb.String(q)

	message := &proto.Msg{}
	message.Query = query

	response, err := c.sendRecv(message)
	if err != nil {
		return nil, err
	}

	return ProtocolBuffersToEvents(response.GetEvents()), nil
}

func (c *GorymanClient) sendRecv(m *proto.Msg) (*proto.Msg, error) {
	return c.tcp.SendRecv(m)
}

func (c *GorymanClient) sendMaybeRecv(m *proto.Msg) (*proto.Msg, error) {
	_, err := c.udp.SendMaybeRecv(m)
	if err != nil {
		return c.tcp.SendMaybeRecv(m)
	}
	return nil, nil
}
