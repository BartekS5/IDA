package logger

import (
	"io"
	"log"
	"os"
)

var (
	InfoLog  *log.Logger
	ErrorLog *log.Logger
	WarnLog  *log.Logger
	logFile  *os.File
)

const (
	INFO = iota
	DEBUG
)

// InitLogger initializes the logger with a file output and console output
func InitLogger(filename string, level int) error {
	var err error
	logFile, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	multiWriter := io.MultiWriter(os.Stdout, logFile)

	InfoLog = log.New(multiWriter, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLog = log.New(multiWriter, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	WarnLog = log.New(multiWriter, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile)

	return nil
}

func Close() {
	if logFile != nil {
		logFile.Close()
	}
}

// Helper functions (kept for backward compatibility with other files)
func Init() {
	InfoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLog = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	WarnLog = log.New(os.Stdout, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func Info(format string, v ...interface{}) {
	if InfoLog == nil { Init() }
	InfoLog.Printf(format, v...)
}

func Infof(format string, v ...interface{}) {
	Info(format, v...)
}

func Error(format string, v ...interface{}) {
	if ErrorLog == nil { Init() }
	ErrorLog.Printf(format, v...)
}

func Errorf(format string, v ...interface{}) {
	Error(format, v...)
}

func Warn(format string, v ...interface{}) {
	if WarnLog == nil { Init() }
	WarnLog.Printf(format, v...)
}

func Warnf(format string, v ...interface{}) {
	Warn(format, v...)
}
