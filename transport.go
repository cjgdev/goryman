package goryman

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	pb "code.google.com/p/goprotobuf/proto"
)

const MAX_UDP_SIZE = 16384

func SendTcp(message *proto.Msg, conn net.Conn) (*proto.Msg, error) {
	msg := &proto.Msg{}
	data, err := pb.Marshal(message)
	if err != nil {
		return msg, err
	}
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, uint32(len(data))); err != nil {
		return msg, err
	}
	if _, err = conn.Write(b.Bytes()); err != nil {
		return msg, err
	}
	if _, err = conn.Write(data); err != nil {
		return msg, err
	}
	var header uint32
	if err = binary.Read(conn, binary.BigEndian, &header); err != nil {
		return msg, err
	}
	response := make([]byte, header)
	if err = readResponse(conn, response); err != nil {
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

func SendUdp(message *proto.Msg, conn net.Conn) (*proto.Msg, error) {
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

func readResponse(r io.Reader, p []byte) error {
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			return err
		}
	}
	return nil
}
