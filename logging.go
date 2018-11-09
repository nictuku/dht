package dht

type DebugLogger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type nullLogger struct{}

func (l *nullLogger) Debugf(format string, args ...interface{}) {}
func (l *nullLogger) Infof(format string, args ...interface{})  {}
func (l *nullLogger) Errorf(format string, args ...interface{}) {}
