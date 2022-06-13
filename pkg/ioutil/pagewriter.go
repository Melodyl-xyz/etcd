// Copyright 2016 The etcd Authors
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

package ioutil

import (
	"io"
)

// 128kb
var defaultBufferBytes = 128 * 1024

// PageWriter implements the io.Writer interface so that writes will
// either be in page chunks or from flushing.
type PageWriter struct {
	w io.Writer
	// pageOffset tracks the page offset of the base of the buffer
	pageOffset int
	// pageBytes is the number of bytes per page
	// wal使用的时候传入的是4k
	pageBytes int
	// bufferedBytes counts the number of bytes pending for write in the buffer
	// 当前有多少的数据还没写入
	bufferedBytes int
	// buf holds the write buffer
	buf []byte
	// bufWatermarkBytes is the number of bytes the buffer can hold before it needs
	// to be flushed. It is less than len(buf) so there is space for slack writes
	// to bring the writer to page alignment.
	// bufWatermarkBytes是缓冲区在需要刷新之前可以保留的字节数。它
	// 小于len（buf），因此有空间进行松弛写入，以使写入程序与页面对齐。
	// 默认是128kb
	bufWatermarkBytes int
}

// NewPageWriter creates a new PageWriter. pageBytes is the number of bytes
// to write per page. pageOffset is the starting offset of io.Writer.
// wal使用的时候pageBytes使用的是4k；w使用的是file。
func NewPageWriter(w io.Writer, pageBytes, pageOffset int) *PageWriter {
	return &PageWriter{
		w:                 w,
		pageOffset:        pageOffset,
		pageBytes:         pageBytes,
		buf:               make([]byte, defaultBufferBytes+pageBytes),
		bufWatermarkBytes: defaultBufferBytes,
	}
}

// 返回写入了多少的长度和error
func (pw *PageWriter) Write(p []byte) (n int, err error) {
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		// no overflow
		// 没有溢出。即 写入的数据长度+当前已经写入的buffer <= 允许保留的大小
		// 直接用copy p到写入buf中
		copy(pw.buf[pw.bufferedBytes:], p)
		pw.bufferedBytes += len(p)
		return len(p), nil
	}
	// complete the slack page in the buffer if unaligned
	// 到这，说明数据+未写入的已经溢出了。
	// 即 len(p)+pw.bufferedBytes > pw.bufWatermarkBytes
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	if slack != pw.pageBytes {
		// partial为true，则说明p的大小不足以填补page对齐的空洞
		partial := slack > len(p)
		if partial {
			// not enough data to complete the slack page
			// 也防止待会 p = p[slack:]溢出了
			slack = len(p)
		}
		// special case: writing to slack page in buffer
		// slack <= len(p)，即把p的slack前一部分先写入进去
		copy(pw.buf[pw.bufferedBytes:], p[:slack])
		pw.bufferedBytes += slack
		n = slack
		p = p[slack:]
		if partial {
			// avoid forcing an unaligned flush
			// 下面就要flush了，但是这里还没对齐到4k，所以就直接返回
			return n, nil
		}
	}
	// buffer contents are now page-aligned; clear out
	// pageOffset已经对齐了，且 现在还有一部分数据没写入。
	// 这部分数据可能是已经被截断的。
	// 执行flush，把所有对齐的数据write到磁盘，这个时候能完整的占满一个pagecache。
	// flush也会把pageWriter的位置标记置为0
	if err = pw.Flush(); err != nil {
		return n, err
	}
	// directly write all complete pages without copying
	if len(p) > pw.pageBytes {
		// 先计算有多少个页
		pages := len(p) / pw.pageBytes
		// 然后先写入这么多页
		c, werr := pw.w.Write(p[:pages*pw.pageBytes])
		n += c
		if werr != nil {
			return n, werr
		}
		// 然后截断p
		p = p[pages*pw.pageBytes:]
	}
	// write remaining tail to buffer
	// 后面的p长度多不能对齐了，就直接写到buffer中。
	c, werr := pw.Write(p)
	n += c
	return n, werr
}

// Flush flushes buffered data.
func (pw *PageWriter) Flush() error {
	_, err := pw.flush()
	return err
}

// FlushN flushes buffered data and returns the number of written bytes.
func (pw *PageWriter) FlushN() (int, error) {
	return pw.flush()
}

// flush会重置pageOffset和bufferedBytes
func (pw *PageWriter) flush() (int, error) {
	if pw.bufferedBytes == 0 {
		return 0, nil
	}
	n, err := pw.w.Write(pw.buf[:pw.bufferedBytes])
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes
	pw.bufferedBytes = 0
	return n, err
}
