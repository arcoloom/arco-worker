package runtime

import (
	"bytes"
	"sync"
)

type lineEmitterWriter struct {
	stream  LogStream
	emitter LogEmitter

	mu     sync.Mutex
	buffer bytes.Buffer
}

func newLineEmitterWriter(stream LogStream, emitter LogEmitter) *lineEmitterWriter {
	return &lineEmitterWriter{
		stream:  stream,
		emitter: emitter,
	}
}

func (w *lineEmitterWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.emitter == nil {
		return len(p), nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.buffer.Write(p); err != nil {
		return 0, err
	}
	w.flushLocked(false)
	return len(p), nil
}

func (w *lineEmitterWriter) Flush() {
	if w.emitter == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.flushLocked(true)
}

func (w *lineEmitterWriter) flushLocked(flushPartial bool) {
	for {
		data := w.buffer.Bytes()
		index := bytes.IndexByte(data, '\n')
		if index < 0 {
			break
		}

		w.emitter(LogEntry{
			Stream: w.stream,
			Line:   string(data[:index]),
		})
		w.buffer.Next(index + 1)
	}

	if flushPartial && w.buffer.Len() > 0 {
		w.emitter(LogEntry{
			Stream: w.stream,
			Line:   w.buffer.String(),
		})
		w.buffer.Reset()
	}
}
