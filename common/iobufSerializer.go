package common

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
	"io"
	"rcstor/rpc"
	"reflect"
)

const IOBufMagic byte = 0xc7
const GOBMagic byte = 0xc6
const NilMagic byte = 0xc5

type IOBufServerCodec struct {
	rwc    io.ReadWriteCloser
	reader *bufio.Reader
	pool   *IOBufferPool
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
	closed bool
}

func (c *IOBufServerCodec) ReadRequestHeader(r *rpc.Request) error {
	return c.dec.Decode(r)
}

func (c *IOBufServerCodec) ReadRequestBody(body interface{}) error {
	magic, err := c.reader.ReadByte()
	if err != nil {
		log.Errorln("Can not read magic")
		return err
	}

	if magic == GOBMagic {
		err = c.dec.Decode(body)
		if err != nil {
			log.Errorln("rpc: decode error:", err)
			return err
		}

		v := reflect.ValueOf(body)
		if v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.Struct {

			fields := 0

			//TODO: Alloc multiple buffers as an atomic action.
			for i := 0; i < v.Elem().NumField(); i++ {
				field := v.Elem().Field(i)
				if field.Type() == reflect.TypeOf(IOBuffer{}) {
					fields++

					if fields >= 2 {
						log.Warnln("Multiple IOBuffer in 1 rpc: deadlock possible.")
					}

					var size int
					bs := make([]byte, 4)

					n, err := c.reader.Read(bs)
					if err != nil && n != 4 {
						log.Errorln("rpc: unable to get size of iobuf:", err)
						return err
					}

					size = int(binary.LittleEndian.Uint32(bs))
					buf := c.pool.GetBuffer(size)
					buf.pool = c.pool

					read := 0
					for read != size {
						n, err = c.reader.Read(buf.Data[read:])
						if err != nil {
							return err
						}
						read += n
					}

					field.Set(reflect.ValueOf(buf))
				}
			}
		}

		return nil

	} else if magic == IOBufMagic {
		var size int
		bs := make([]byte, 4)

		n, err := c.reader.Read(bs)
		if err != nil && n != 4 {
			log.Errorln("rpc: unable to get size of iobuf:", err)
			return err
		}

		size = int(binary.LittleEndian.Uint32(bs))
		buf := c.pool.GetBuffer(size)

		read := 0
		for read != size {
			n, err = c.reader.Read(buf.Data[read:])
			if err != nil {
				return err
			}
			read += n
		}
		reflect.ValueOf(body).Elem().Set(reflect.ValueOf(buf))

		return nil
	} else if magic == NilMagic {
		//reflect.ValueOf(body).Set(reflect.ValueOf(nil))
		return nil
	}

	log.Errorln("rpc: unrecognized magic number", magic)
	return ErrUnrecognizedMagic

}

func (c *IOBufServerCodec) WriteResponse(r *rpc.Response, body interface{}) (err error) {

	if err = c.enc.Encode(r); err != nil {
		if c.encBuf.Flush() == nil {
			// Gob couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			log.Errorln("rpc: gob error encoding response:", err)
			c.Close()
		}
		return
	}

	if body == nil {
		log.Infoln("Begin to write nil magic")
		err = c.encBuf.WriteByte(NilMagic)
		if err != nil {
			log.Errorln("rpc: write magic number for gob error:", err)
			c.Close()
			return
		}
		return c.encBuf.Flush()

	}

	v := reflect.ValueOf(body)

	//Begin to write IOBuffer, zero copyed encoding.
	if v.Type() == reflect.PtrTo(reflect.TypeOf(IOBuffer{})) {

		err = c.encBuf.WriteByte(IOBufMagic)
		if err != nil {
			log.Errorln("rpc: write magic number for iobuf error:", err)
			c.Close()
			return
		}
		data := reflect.ValueOf(body).Elem().FieldByName("Data").Bytes()
		var n int

		size := len(data)

		toSend := int(reflect.ValueOf(body).Elem().FieldByName("bytesToSend").Int())

		bs := make([]byte, 8)
		binary.LittleEndian.PutUint32(bs, uint32(size))
		binary.LittleEndian.PutUint32(bs[4:], uint32(toSend))

		n, err = c.encBuf.Write(bs)

		if err != nil && n != 8 {
			log.Errorln("rpc: write len of data error:", err)
			c.Close()
			return
		}

		n, err = c.encBuf.Write(data[:toSend])
		if n != toSend || err != nil {
			log.Errorln("rpc: write iobuf data error:", err)
			c.Close()
			return
		}

		defer reflect.ValueOf(body).MethodByName("Unref").Call([]reflect.Value{})

		return c.encBuf.Flush()

	} else {
		err = c.encBuf.WriteByte(GOBMagic)
		if err != nil {
			log.Errorln("rpc: write magic number for gob error:", err)
			c.Close()
			return
		}

		if err = c.enc.Encode(body); err != nil {
			if c.encBuf.Flush() == nil {
				// Was a gob problem encoding the body but the header has been written.
				// Shut down the connection to signal that the connection is broken.
				log.Errorln("rpc: gob error encoding body:", err)
				log.Errorln("Body is",body)
				c.Close()
			}
			return
		}
		return c.encBuf.Flush()
	}

}

func (c *IOBufServerCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}

type IOBufClientCodec struct {
	rwc       io.ReadWriteCloser
	reader    *bufio.Reader
	dec       *gob.Decoder
	enc       *gob.Encoder
	encBuf    *bufio.Writer
	iobufPool *IOBufferPool
}

func (c *IOBufClientCodec) WriteRequest(r *rpc.Request, body interface{}) (err error) {
	if err = c.enc.Encode(r); err != nil {
		return
	}

	if body == nil {
		err = c.encBuf.WriteByte(NilMagic)
		if err != nil {
			log.Errorln("rpc: write magic number for gob error:", err)
			c.Close()
			return
		}
		return c.encBuf.Flush()

	}
	v := reflect.ValueOf(body)

	//Begin to write IOBuffer, zero copyed encoding.
	if v.Type() == reflect.PtrTo(reflect.TypeOf(IOBuffer{})) {
		err = c.encBuf.WriteByte(IOBufMagic)
		if err != nil {
			log.Errorln("rpc: write magic number for iobuf error:", err)
			c.Close()
			return
		}
		data := reflect.ValueOf(body).Elem().FieldByName("Data").Bytes()
		var n int

		size := len(data)
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, uint32(size))

		n, err = c.encBuf.Write(bs)

		if err != nil && n != 4 {
			log.Errorln("rpc: write len of data error:", err)
			c.Close()
			return
		}

		n, err = c.encBuf.Write(data)
		if n != len(data) || err != nil {
			log.Errorln("rpc: write iobuf data error:", err)
			c.Close()
			return
		}

		return c.encBuf.Flush()

	} else {
		var empty IOBuffer

		buffers := make([]IOBuffer, 0)

		var dataInGob interface{}

		if v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.Struct {

			indirect := reflect.Indirect(v)

			k := reflect.New(indirect.Type())
			k.Elem().Set(reflect.ValueOf(indirect.Interface()))
			dataInGob = k.Interface()

			for i := 0; i < k.Elem().NumField(); i++ {
				field := k.Elem().Field(i)

				if field.Type() == reflect.TypeOf(IOBuffer{}) {
					buffers = append(buffers, field.Interface().(IOBuffer))
					field.Set(reflect.ValueOf(empty))
				}
			}
		} else {
			dataInGob = body
		}

		err = c.encBuf.WriteByte(GOBMagic)
		if err != nil {
			log.Errorln("rpc: write magic number for gob error:", err)
			c.Close()
			return
		}

		if err = c.enc.Encode(dataInGob); err != nil {
			if c.encBuf.Flush() == nil {
				// Was a gob problem encoding the body but the header has been written.
				// Shut down the connection to signal that the connection is broken.
				log.Errorln("rpc: gob error encoding body:", err)
				c.Close()
			}
		}

		err = c.encBuf.Flush()
		if err != nil {
			log.Errorln("rpc: flush error:", err)
			c.Close()
			return
		}

		for i := 0; i < len(buffers); i++ {
			data := buffers[i].Data
			var n int

			size := len(data)
			bs := make([]byte, 4)
			binary.LittleEndian.PutUint32(bs, uint32(size))

			n, err = c.encBuf.Write(bs)

			if err != nil && n != 4 {
				log.Errorln("rpc: write len of data error:", err)
				c.Close()
				return
			}

			n, err = c.encBuf.Write(data)
			if n != len(data) || err != nil {
				log.Errorln("rpc: write iobuf data error:", err)
				c.Close()
				return
			}

			err = c.encBuf.Flush()
			if err != nil {
				log.Errorln("rpc: flush error:", err)
				c.Close()
				return
			}
		}

		return
	}
}

func (c *IOBufClientCodec) ReadResponseHeader(r *rpc.Response) error {
	return c.dec.Decode(r)
}

func (c *IOBufClientCodec) ReadResponseBody(body interface{}) error {

	magic, err := c.reader.ReadByte()
	if err != nil {
		log.Errorln("Can not read magic")
		return err
	}

	if magic == GOBMagic {
		return c.dec.Decode(body)
	} else if magic == IOBufMagic {
		var size int
		bs := make([]byte, 8)

		n, err := c.reader.Read(bs)
		if err != nil && n != 8 {
			log.Errorln("rpc: unable to get size of iobuf:", err)
			return err
		}

		size = int(binary.LittleEndian.Uint32(bs[:4]))
		sended := int(binary.LittleEndian.Uint32(bs[4:]))

		buf := c.iobufPool.GetBuffer(size)
		//log.Warn("ResponsBody Size:", size, " Sended:", sended)

		read := 0
		for read != sended {
			n, err = c.reader.Read(buf.Data[read:])
			if err != nil {
				log.Error(err)
				return err
			}
			read += n
		}

		for i := range buf.Data[read:] {
			buf.Data[i+read] = 0
		}

		reflect.ValueOf(body).Elem().Set(reflect.ValueOf(buf))

		return nil
	} else if magic == NilMagic{
		log.Infoln("Begin to read nil magic")
		//reflect.ValueOf(body).Set(reflect.ValueOf(nil))
		return nil
	}

	log.Errorln("rpc: unrecognized magic number", magic)
	return ErrUnrecognizedMagic
}

func (c *IOBufClientCodec) Close() error {
	return c.rwc.Close()
}
