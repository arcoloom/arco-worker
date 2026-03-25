package runtime

import "testing"

func TestLineEmitterWriterFlushesTrailingLine(t *testing.T) {
	t.Parallel()

	var entries []LogEntry
	writer := newLineEmitterWriter(LogStreamStdout, func(entry LogEntry) {
		entries = append(entries, entry)
	})

	if _, err := writer.Write([]byte("first\nsecond")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	writer.Flush()

	if len(entries) != 2 {
		t.Fatalf("emitted %d entries, want 2", len(entries))
	}
	if entries[0].Stream != LogStreamStdout || entries[0].Line != "first" {
		t.Fatalf("first entry = %+v, want stdout/first", entries[0])
	}
	if entries[1].Stream != LogStreamStdout || entries[1].Line != "second" {
		t.Fatalf("second entry = %+v, want stdout/second", entries[1])
	}
}
