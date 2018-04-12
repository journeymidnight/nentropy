package log

import (
	"fmt"
)

type RaftLogger struct {
	log *Logger
}

func NewRaftLogger(log *Logger) *RaftLogger {
	return &RaftLogger{log: log}
}

func (r *RaftLogger) Debug(v ...interface{}) {
	if r.log.GetLevel() >= 10 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintln(v...)))
	}
}

func (r *RaftLogger) Debugf(format string, v ...interface{}) {
	if r.log.GetLevel() >= 10 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintf(format, v...)))
	}
}

func (r *RaftLogger) Info(v ...interface{}) {
	if r.log.GetLevel() >= 10 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintln(v...)))
	}
}

func (r *RaftLogger) Infof(format string, v ...interface{}) {
	if r.log.GetLevel() >= 10 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintf(format, v...)))
	}
}

func (r *RaftLogger) Warning(v ...interface{}) {
	if r.log.GetLevel() >= 5 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintln(v...)))
	}
}

func (r *RaftLogger) Warningf(format string, v ...interface{}) {
	if r.log.GetLevel() >= 5 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintf(format, v...)))
	}
}

func (r *RaftLogger) Error(v ...interface{}) {
	if r.log.GetLevel() >= 3 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintln(v...)))
	}
}

func (r *RaftLogger) Errorf(format string, v ...interface{}) {
	if r.log.GetLevel() >= 3 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintf(format, v...)))
	}
}

func (r *RaftLogger) Fatal(v ...interface{}) {
	if r.log.GetLevel() >= 1 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintln(v...)))
	}
}

func (r *RaftLogger) Fatalf(format string, v ...interface{}) {
	if r.log.GetLevel() >= 1 {
		r.log.Logger.Output(2, header("raft:", fmt.Sprintf(format, v...)))
	}
}

func (r *RaftLogger) Panic(v ...interface{}) {
	s := header("raft:", fmt.Sprintln(v...))
	r.log.Logger.Output(2, s)
	panic(s)
}

func (r *RaftLogger) Panicf(format string, v ...interface{}) {
	s := header("raft:", fmt.Sprintf(format, v...))
	r.log.Logger.Output(2, s)
	panic(s)
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s %s", lvl, msg)
}
