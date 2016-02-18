package pwr

import "fmt"

// ProgressCallback is called periodically to announce the degree of completeness of an operation
type ProgressCallback func(percent float64)

type ProgressLabelCallback func(label string)

type MessageCallback func(level, msg string)

type StateConsumer struct {
	OnProgress      ProgressCallback
	OnProgressLabel ProgressLabelCallback
	OnMessage       MessageCallback
}

func (sc *StateConsumer) Progress(percent float64) {
	if sc.OnProgress != nil {
		sc.OnProgress(percent)
	}
}

func (sc *StateConsumer) ProgressLabel(label string) {
	if sc.OnProgressLabel != nil {
		sc.OnProgressLabel(label)
	}
}

func (sc *StateConsumer) Debug(msg string) {
	if sc.OnMessage != nil {
		sc.OnMessage("debug", msg)
	}
}

func (sc *StateConsumer) Debugf(msg string, args ...interface{}) {
	if sc.OnMessage != nil {
		sc.OnMessage("debug", fmt.Sprintf(msg, args...))
	}
}

func (sc *StateConsumer) Info(msg string) {
	if sc.OnMessage != nil {
		sc.OnMessage("info", msg)
	}
}

func (sc *StateConsumer) Infof(msg string, args ...interface{}) {
	if sc.OnMessage != nil {
		sc.OnMessage("info", fmt.Sprintf(msg, args...))
	}
}

func (sc *StateConsumer) Warn(msg string) {
	if sc.OnMessage != nil {
		sc.OnMessage("warning", msg)
	}
}

func (sc *StateConsumer) Warnf(msg string, args ...interface{}) {
	if sc.OnMessage != nil {
		sc.OnMessage("warning", fmt.Sprintf(msg, args...))
	}
}
