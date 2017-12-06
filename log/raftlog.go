package log

import "fmt"

type RaftLogger struct {
	log *Logger
}

func NewRaftLogger(log *Logger) *RaftLogger {
	return &RaftLogger{log: log}
}

func (r *RaftLogger) Debug(v ...interface{}) {
	r.log.Println(20, v)
}

func (r *RaftLogger) Debugf(format string, v ...interface{}) {
	r.log.Printf(20, format, v)
}

func (r *RaftLogger) Info(v ...interface{}) {
	r.log.Println(10, v)
}

func (r *RaftLogger) Infof(format string, v ...interface{}) {
	r.log.Printf(10, format, v)
}

func (r *RaftLogger) Warning(v ...interface{}) {
	r.log.Println(5, v)
}

func (r *RaftLogger) Warningf(format string, v ...interface{}) {
	r.log.Printf(5, format, v)
}

func (r *RaftLogger) Error(v ...interface{}) {
	r.log.Println(3, v)
}

func (r *RaftLogger) Errorf(format string, v ...interface{}) {
	r.log.Printf(3, format, v)
}

func (r *RaftLogger) Fatal(v ...interface{}) {
	r.log.Println(1, v)
}

func (r *RaftLogger) Fatalf(format string, v ...interface{}) {
	r.log.Printf(1, format, v)
}

func (r *RaftLogger) Panic(v ...interface{}) {
	s := fmt.Sprint(v...)
	r.log.Println(0, s)
	panic(s)
}

func (r *RaftLogger) Panicf(format string, v ...interface{}) {
	r.log.Panicf(5, format, v)
}
