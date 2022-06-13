// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"go.etcd.io/etcd/pkg/v3/crc"
	"go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
// walPageBytes 是为了将record flush到真实writer的对齐字段。
// 它应该是最小扇区的整数倍，这样WAL可以区分损坏写入（torn writes）和一般的数据损坏（data corruption）。
// 这里是4k。
// wal: use page buffered writer for writing records
// 强制文件分裂仅发生在扇区边界上。
const walPageBytes = 8 * minSectorSize

type encoder struct {
	mu sync.Mutex
	bw *ioutil.PageWriter

	crc       hash.Hash32
	// 1M的buf，初始化的时候创建，后面每次写的时候小于1M的数据都可以放到这里面，减少内存分配次数。
	// buf和uint64buf的目标都是一致的
	buf       []byte
	uint64buf []byte
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder creates a new encoder with current file offset for the page writer.
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 计算record的crc，赋值给record pb
	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
		n    int
	)

	// 如果size这个buf接得住，就放到buf里面；
	// 如果size这个buf接不住，则Marshal()会处理来处理。
	// 优化：这有个好处就是减少了byte分配。（wal: reduce allocation when encoding entries）
	if rec.Size() > len(e.buf) {
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else {
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}
	// 强制 8 字节对齐，计算lenField的大小和需要填充的PadBytes大小
	lenField, padBytes := encodeFrameSize(len(data))
	// 将lenField写入到PageWriter中
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}
	// 将data填充后写入到PageWriter中
	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	n, err = e.bw.Write(data)
	walWriteBytes.Add(float64(n)) // prometheus监控
	return err
}

func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

func (e *encoder) flush() error {
	e.mu.Lock()
	n, err := e.bw.FlushN()
	e.mu.Unlock()
	walWriteBytes.Add(float64(n))
	return err
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	// http://golang.org/src/encoding/binary/binary.go
	// 最初的版本 binary.Write(w, binary.LittleEndian, n)
	// 优化：在binary.Write中会先分配一个byte，然后在put，为了减少二次分配，直接使用PutUin
	binary.LittleEndian.PutUint64(buf, n)
	nv, err := w.Write(buf)
	// prometheus监控
	walWriteBytes.Add(float64(nv))
	return err
}
