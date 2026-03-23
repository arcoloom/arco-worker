package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerRuntime "github.com/arcoloom/arco-worker/internal/worker/runtime"
)

type TaskLogRelay struct {
	mu      sync.Mutex
	session ControlPlaneSession
	taskID  string
	nextSeq uint64
	buffer  []queuedTaskLog
	ready   bool
	warned  bool
}

type queuedTaskLog struct {
	source workerv1.LogSource
	stream workerv1.LogStream
	line   string
}

func NewTaskLogRelay() *TaskLogRelay {
	return &TaskLogRelay{}
}

func (r *TaskLogRelay) WorkerWriter() io.Writer {
	return &taskLogWriter{
		relay:  r,
		source: workerv1.LogSource_LOG_SOURCE_WORKER,
	}
}

func (r *TaskLogRelay) ProgramEmitter() workerRuntime.LogEmitter {
	return func(entry workerRuntime.LogEntry) {
		r.enqueue(
			workerv1.LogSource_LOG_SOURCE_PROGRAM,
			programStreamToProto(entry.Stream),
			entry.Line,
		)
	}
}

func (r *TaskLogRelay) BindTask(session ControlPlaneSession, taskID string) {
	if r == nil || session == nil || strings.TrimSpace(taskID) == "" {
		return
	}

	r.mu.Lock()
	r.session = session
	r.taskID = strings.TrimSpace(taskID)
	buffered := append([]queuedTaskLog(nil), r.buffer...)
	r.buffer = nil
	r.mu.Unlock()

	r.flushBuffered(buffered)
	for {
		r.mu.Lock()
		if len(r.buffer) == 0 {
			r.ready = true
			r.mu.Unlock()
			return
		}
		buffered = append([]queuedTaskLog(nil), r.buffer...)
		r.buffer = nil
		r.mu.Unlock()
		r.flushBuffered(buffered)
	}
}

func (r *TaskLogRelay) enqueue(source workerv1.LogSource, stream workerv1.LogStream, line string) {
	if r == nil {
		return
	}

	trimmed := strings.TrimRight(line, "\r")
	entry := queuedTaskLog{
		source: source,
		stream: stream,
		line:   trimmed,
	}

	r.mu.Lock()
	ready := r.ready && r.session != nil && r.taskID != ""
	if !ready {
		r.buffer = append(r.buffer, entry)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	r.send(entry)
}

func (r *TaskLogRelay) send(entry queuedTaskLog) {
	r.mu.Lock()
	session := r.session
	taskID := r.taskID
	if session == nil || taskID == "" {
		r.buffer = append(r.buffer, entry)
		r.mu.Unlock()
		return
	}
	r.nextSeq++
	sequence := r.nextSeq
	r.mu.Unlock()

	err := session.Send(context.Background(), &workerv1.WorkerToControl{
		Message: &workerv1.WorkerToControl_Log{
			Log: &workerv1.TaskLog{
				TaskId:   taskID,
				Source:   entry.source,
				Stream:   entry.stream,
				Line:     entry.line,
				Sequence: sequence,
			},
		},
	})
	if err == nil {
		return
	}

	r.mu.Lock()
	alreadyWarned := r.warned
	r.warned = true
	r.mu.Unlock()
	if alreadyWarned {
		return
	}

	fmt.Fprintf(os.Stderr, "task log relay failed: %v\n", err)
}

func (r *TaskLogRelay) flushBuffered(items []queuedTaskLog) {
	for _, item := range items {
		r.send(item)
	}
}

func programStreamToProto(stream workerRuntime.LogStream) workerv1.LogStream {
	switch stream {
	case workerRuntime.LogStreamStdout:
		return workerv1.LogStream_LOG_STREAM_STDOUT
	case workerRuntime.LogStreamStderr:
		return workerv1.LogStream_LOG_STREAM_STDERR
	default:
		return workerv1.LogStream_LOG_STREAM_UNSPECIFIED
	}
}

type taskLogWriter struct {
	relay  *TaskLogRelay
	source workerv1.LogSource

	mu     sync.Mutex
	buffer bytes.Buffer
}

func (w *taskLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.buffer.Write(p); err != nil {
		return 0, err
	}

	for {
		data := w.buffer.Bytes()
		index := bytes.IndexByte(data, '\n')
		if index < 0 {
			return len(p), nil
		}

		line := string(data[:index])
		w.relay.enqueue(w.source, workerv1.LogStream_LOG_STREAM_UNSPECIFIED, line)
		w.buffer.Next(index + 1)
	}
}
