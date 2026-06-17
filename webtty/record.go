package webtty

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RecordFrame struct {
	Delay int64  `json:"delay"`
	Data  string `json:"data"`
}

type Recorder struct {
	mu       sync.Mutex
	file     *os.File
	encoder  *json.Encoder
	frames   []RecordFrame
	lastTime time.Time
}

func DefaultRecordFilePath() string {
	home := os.Getenv("HOME")
	if home == "" {
		home = "."
	}
	dir := filepath.Join(home, ".gotty", "records")
	os.MkdirAll(dir, 0755)
	timestamp := time.Now().Format("20060102_150405")
	return filepath.Join(dir, timestamp+".json")
}

func NewRecorder(filePath string) (*Recorder, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create record directory: %v", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create record file: %v", err)
	}

	return &Recorder{
		file:     f,
		encoder:  json.NewEncoder(f),
		frames:   make([]RecordFrame, 0),
		lastTime: time.Now(),
	}, nil
}

func (r *Recorder) Record(data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	delay := now.Sub(r.lastTime).Milliseconds()
	r.lastTime = now

	frame := RecordFrame{
		Delay: delay,
		Data:  base64.StdEncoding.EncodeToString(data),
	}
	r.frames = append(r.frames, frame)
}

func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	defer r.file.Close()

	err := json.NewEncoder(r.file).Encode(r.frames)
	if err != nil {
		return fmt.Errorf("failed to write record frames: %v", err)
	}

	return nil
}

type Player struct {
	frames []RecordFrame
	index  int
}

func NewPlayer(filePath string) (*Player, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open record file: %v", err)
	}
	defer f.Close()

	var frames []RecordFrame
	if err := json.NewDecoder(f).Decode(&frames); err != nil {
		return nil, fmt.Errorf("failed to decode record file: %v", err)
	}

	return &Player{
		frames: frames,
		index:  0,
	}, nil
}

func (p *Player) HasNext() bool {
	return p.index < len(p.frames)
}

func (p *Player) Next() (RecordFrame, bool) {
	if p.index >= len(p.frames) {
		return RecordFrame{}, false
	}
	frame := p.frames[p.index]
	p.index++
	return frame, true
}

func (p *Player) TotalFrames() int {
	return len(p.frames)
}
