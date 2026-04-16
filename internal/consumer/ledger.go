package consumer

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Ledger struct {
	enabled bool
	path    string
	mu      sync.Mutex
	seen    map[string]struct{}
}

func NewLedger(mode, path string) (*Ledger, error) {
	l := &Ledger{
		enabled: mode == "on",
		path:    path,
		seen:    map[string]struct{}{},
	}
	if !l.enabled {
		return l, nil
	}
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Ledger) Enabled() bool { return l.enabled }

func (l *Ledger) Seen(eventID string) bool {
	if !l.enabled || eventID == "" {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[eventID]; ok {
		return true
	}
	return false
}

func (l *Ledger) Record(eventID string) error {
	if !l.enabled || eventID == "" {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[eventID]; ok {
		return nil
	}
	l.seen[eventID] = struct{}{}
	return l.append(eventID)
}

func (l *Ledger) append(eventID string) error {
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(map[string]string{"event_id": eventID})
}

func (l *Ledger) load() error {
	f, err := os.Open(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var row map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			continue
		}
		if id := row["event_id"]; id != "" {
			l.seen[id] = struct{}{}
		}
	}
	return scanner.Err()
}
