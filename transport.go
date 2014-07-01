package goryman

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	pb "code.google.com/p/goprotobuf/proto"
)

type Transport interface {
	SendRecv(message *proto.Msg) (*proto.Msg, error)
	SendMaybeRecv(message *proto.Msg) (*proto.Msg, error)
}

type TcpTransport struct {
	conn         net.Conn
	requestQueue chan request
}

type UdpTransport struct {
	conn         net.Conn
	requestQueue chan request
}

type request struct {
	message     *proto.Msg
	response_ch chan response
}

type response struct {
	message *proto.Msg
	err     error
}

const MAX_UDP_SIZE = 16384

func NewTcpTransport(conn net.Conn) *TcpTransport {
	t := &TcpTransport{
		conn:         conn,
		messageQueue: make(chan request),
	}
	go t.runRequestQueue()
	return t
}

func NewUdpTransport(conn net.Conn) *UdpTransport {
	t := &TcpTransport{
		conn:         conn,
		messageQueue: make(chan request),
	}
	go t.runRequestQueue()
	return t
}

func (t *TcpTransport) SendRecv(message *proto.Msg) (*proto.Msg, error) {
	response_ch := make(chan response)
	requestQueue <- request{message, response_ch}
	r := <-response_ch
	return r.message, r.err
}

func (t *TcpTransport) SendMaybeRecv(message *proto.Msg) (*proto.Msg, error) {
	return t.SendRecv(message)
}

func (t *TcpTransport) Close() error {
	close(t.requestQueue)
	err := t.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (t *TcpTransport) runRequestQueue() {
	for req := range t.requestQueue {
		message := req.message
		response_ch := req.response_ch

		msg, err := t.execRequest(message)

		response_ch <- response{msg, err}
	}
}

func (t *TcpTransport) execRequest(message *proto.Msg) (*proto.Msg, error) {
	msg := &proto.Msg{}
	data, err := pb.Marshal(message)
	if err != nil {
		return msg, err
	}
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, uint32(len(data))); err != nil {
		return msg, err
	}
	if _, err = t.conn.Write(b.Bytes()); err != nil {
		return msg, err
	}
	if _, err = t.conn.Write(data); err != nil {
		return msg, err
	}
	var header uint32
	if err = binary.Read(t.conn, binary.BigEndian, &header); err != nil {
		return msg, err
	}
	response := make([]byte, header)
	if err = readMessages(t.conn, response); err != nil {
		return msg, err
	}
	if err = pb.Unmarshal(response, msg); err != nil {
		return msg, err
	}
	if msg.GetOk() != true {
		return msg, errors.New(msg.GetError())
	}
	return msg, nil
}

func (t *UdpTransport) SendRecv(message *proto.Msg) (*proto.Msg, error) {
	return nil, fmt.Errorf("udp doesn't support receiving acknowledgements")
}

func (t *UdpTransport) SendMaybeRecv(message *proto.Msg) (*proto.Msg, error) {
	response_ch := make(chan response)
	requestQueue <- request{message, response_ch}
	r := <-response_ch
	return r.message, r.err
}

func (t *UdpTransport) Close() error {
	close(t.requestQueue)
	err := t.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (t *UdpTransport) runRequestQueue() {
	for req := range t.requestQueue {
		message := req.message
		response_ch := req.response_ch

		msg, err := t.execRequest(message)

		response_ch <- response{msg, err}
	}
}

func (t *UdpTransport) execRequest(message *proto.Msg) (*proto.Msg, error) {
	data, err := pb.Marshal(message)
	if err != nil {
		return nil, err
	}
	if len(data) > MAX_UDP_SIZE {
		return nil, fmt.Errorf("unable to send message, too large for udp")
	}
	if _, err = conn.Write(data); err != nil {
		return nil, err
	}
	return nil, nil
}

func readMessages(r io.Reader, p []byte) error {
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			return err
		}
	}
	return nil
}
