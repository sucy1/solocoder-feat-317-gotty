package webtty

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type WebTTY struct {
	masterConn Master
	slave      Slave

	windowTitle []byte
	permitWrite bool
	columns     int
	rows        int
	reconnect   int
	masterPrefs []byte

	recorder *Recorder
	player   *Player

	bufferSize int
	writeMutex sync.Mutex
}

func New(masterConn Master, slave Slave, options ...Option) (*WebTTY, error) {
	wt := &WebTTY{
		masterConn: masterConn,
		slave:      slave,

		permitWrite: false,
		columns:     0,
		rows:        0,

		bufferSize: 1024,
	}

	for _, option := range options {
		option(wt)
	}

	return wt, nil
}

func (wt *WebTTY) Run(ctx context.Context) error {
	if wt.player != nil {
		return wt.runPlayback(ctx)
	}

	err := wt.sendInitializeMessage()
	if err != nil {
		return errors.Wrapf(err, "failed to send initializing message")
	}

	errs := make(chan error, 2)

	go func() {
		errs <- func() error {
			buffer := make([]byte, wt.bufferSize)
			for {
				n, err := wt.slave.Read(buffer)
				if err != nil {
					return ErrSlaveClosed
				}

				err = wt.handleSlaveReadEvent(buffer[:n])
				if err != nil {
					return err
				}
			}
		}()
	}()

	go func() {
		errs <- func() error {
			buffer := make([]byte, wt.bufferSize)
			for {
				n, err := wt.masterConn.Read(buffer)
				if err != nil {
					return ErrMasterClosed
				}

				err = wt.handleMasterReadEvent(buffer[:n])
				if err != nil {
					return err
				}
			}
		}()
	}()

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-errs:
	}

	if wt.recorder != nil {
		wt.recorder.Close()
	}

	return err
}

func (wt *WebTTY) runPlayback(ctx context.Context) error {
	err := wt.sendInitializeMessage()
	if err != nil {
		return errors.Wrapf(err, "failed to send initializing message")
	}

	playCtx, playCancel := context.WithCancel(ctx)
	defer playCancel()

	speed := 1.0
	paused := false
	pauseMu := sync.Mutex{}
	frameInterval := time.Second / 60

	controlChan := make(chan string, 16)

	go func() {
		buffer := make([]byte, wt.bufferSize)
		for {
			n, err := wt.masterConn.Read(buffer)
			if err != nil {
				playCancel()
				return
			}

			if n > 0 {
				msg := string(buffer[:n])
				if len(msg) > 0 && msg[0] == PlayControl {
					controlChan <- msg[1:]
				}
			}
		}
	}()

	for wt.player.HasNext() {
		select {
		case <-playCtx.Done():
			return playCtx.Err()
		case ctrl := <-controlChan:
			switch ctrl {
			case "pause":
				pauseMu.Lock()
				paused = true
				pauseMu.Unlock()
			case "resume":
				pauseMu.Lock()
				paused = false
				pauseMu.Unlock()
			case "speed0.5":
				speed = 0.5
			case "speed1":
				speed = 1.0
			case "speed2":
				speed = 2.0
			case "speed4":
				speed = 4.0
			}
		default:
		}

		pauseMu.Lock()
		isPaused := paused
		pauseMu.Unlock()

		if isPaused {
			select {
			case <-playCtx.Done():
				return playCtx.Err()
			case ctrl := <-controlChan:
				switch ctrl {
				case "resume":
					pauseMu.Lock()
					paused = false
					pauseMu.Unlock()
				case "speed0.5":
					speed = 0.5
				case "speed1":
					speed = 1.0
				case "speed2":
					speed = 2.0
				case "speed4":
					speed = 4.0
				}
			}
			continue
		}

		var accumulatedDelay time.Duration
		var pendingData string

		for wt.player.HasNext() {
			frame, ok := wt.player.Next()
			if !ok {
				break
			}

			frameDelay := time.Duration(float64(time.Duration(frame.Delay)*time.Millisecond) / speed)
			accumulatedDelay += frameDelay
			pendingData += frame.Data

			if accumulatedDelay >= frameInterval {
				break
			}
		}

		if pendingData != "" {
			err := wt.masterWrite(append([]byte{Output}, []byte(pendingData)...))
			if err != nil {
				return errors.Wrapf(err, "failed to send playback frame to master")
			}
		}

		if accumulatedDelay > 0 {
			select {
			case <-playCtx.Done():
				return playCtx.Err()
			case ctrl := <-controlChan:
				switch ctrl {
				case "pause":
					pauseMu.Lock()
					paused = true
					pauseMu.Unlock()
				case "resume":
					pauseMu.Lock()
					paused = false
					pauseMu.Unlock()
				case "speed0.5":
					speed = 0.5
				case "speed1":
					speed = 1.0
				case "speed2":
					speed = 2.0
				case "speed4":
					speed = 4.0
				}
			case <-time.After(accumulatedDelay):
			}
		}
	}

	<-playCtx.Done()
	return playCtx.Err()
}

func (wt *WebTTY) sendInitializeMessage() error {
	err := wt.masterWrite(append([]byte{SetWindowTitle}, wt.windowTitle...))
	if err != nil {
		return errors.Wrapf(err, "failed to send window title")
	}

	if wt.reconnect > 0 {
		reconnect, _ := json.Marshal(wt.reconnect)
		err := wt.masterWrite(append([]byte{SetReconnect}, reconnect...))
		if err != nil {
			return errors.Wrapf(err, "failed to set reconnect")
		}
	}

	if wt.masterPrefs != nil {
		err := wt.masterWrite(append([]byte{SetPreferences}, wt.masterPrefs...))
		if err != nil {
			return errors.Wrapf(err, "failed to set preferences")
		}
	}

	if wt.player != nil {
		err := wt.masterWrite([]byte{SetPlayMode})
		if err != nil {
			return errors.Wrapf(err, "failed to set play mode")
		}
	}

	return nil
}

func (wt *WebTTY) handleSlaveReadEvent(data []byte) error {
	safeMessage := base64.StdEncoding.EncodeToString(data)
	err := wt.masterWrite(append([]byte{Output}, []byte(safeMessage)...))
	if err != nil {
		return errors.Wrapf(err, "failed to send message to master")
	}

	if wt.recorder != nil {
		wt.recorder.Record(data)
	}

	return nil
}

func (wt *WebTTY) masterWrite(data []byte) error {
	wt.writeMutex.Lock()
	defer wt.writeMutex.Unlock()

	_, err := wt.masterConn.Write(data)
	if err != nil {
		return errors.Wrapf(err, "failed to write to master")
	}

	return nil
}

func (wt *WebTTY) handleMasterReadEvent(data []byte) error {
	if len(data) == 0 {
		return errors.New("unexpected zero length read from master")
	}

	switch data[0] {
	case Input:
		if !wt.permitWrite {
			return nil
		}

		if len(data) <= 1 {
			return nil
		}

		_, err := wt.slave.Write(data[1:])
		if err != nil {
			return errors.Wrapf(err, "failed to write received data to slave")
		}

	case Ping:
		err := wt.masterWrite([]byte{Pong})
		if err != nil {
			return errors.Wrapf(err, "failed to return Pong message to master")
		}

	case ResizeTerminal:
		if wt.columns != 0 && wt.rows != 0 {
			break
		}

		if len(data) <= 1 {
			return errors.New("received malformed remote command for terminal resize: empty payload")
		}

		var args argResizeTerminal
		err := json.Unmarshal(data[1:], &args)
		if err != nil {
			return errors.Wrapf(err, "received malformed data for terminal resize")
		}
		rows := wt.rows
		if rows == 0 {
			rows = int(args.Rows)
		}

		columns := wt.columns
		if columns == 0 {
			columns = int(args.Columns)
		}

		wt.slave.ResizeTerminal(columns, rows)
	case PlayControl:
		break
	default:
		return errors.Errorf("unknown message type `%c`", data[0])
	}

	return nil
}

type argResizeTerminal struct {
	Columns float64
	Rows    float64
}
