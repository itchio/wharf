package pwr

import "fmt"

// ProgressCallback is called periodically to announce the degree of completeness of an operation
type ProgressCallback func(percent float64)

type MessageCallback func(level, msg string)

type StateConsumer struct {
	OnProgress ProgressCallback
	OnMessage  MessageCallback
}

func (sc *StateConsumer) Progress(percent float64) {
	if sc.OnProgress != nil {
		sc.OnProgress(percent)
	}
}

func (sc *StateConsumer) Debug(msg string) {
	sc.OnMessage("debug", msg)
}

func (sc *StateConsumer) Debugf(msg string, args ...interface{}) {
	sc.OnMessage("debug", fmt.Sprintf(msg, args...))
}

func (sc *StateConsumer) Info(msg string) {
	sc.OnMessage("info", msg)
}

func (sc *StateConsumer) Infof(msg string, args ...interface{}) {
	sc.OnMessage("info", fmt.Sprintf(msg, args...))
}

func (sc *StateConsumer) Warn(msg string) {
	sc.OnMessage("warning", msg)
}

func (sc *StateConsumer) Warnf(msg string, args ...interface{}) {
	sc.OnMessage("warning", fmt.Sprintf(msg, args...))
}
