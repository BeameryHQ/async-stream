package logging

import (
	"os"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
)

var (
	instance  *logrus.Logger
	once      sync.Once
	logpath   = os.Getenv("LOGGING_PATH")
	logToFile = os.Getenv("LOG_TO_FILE")
	logLevel  = os.Getenv("LOG_LEVEL")
)

//GetLogger ...
func GetLogger() *logrus.Logger {
	once.Do(func() {
		var l = logrus.New()
		if logpath == "" {
			logpath = "logrus.log"
		}
		l.SetLevel(MapLevelToLogrus(logLevel))
		l.Formatter = &logrus.JSONFormatter{}
		l.Out = os.Stdout
		if _, err := os.Stat(logpath); os.IsExist(err) {
			os.Remove(logpath)
		}
		if logToFile != "" {
			file, err := os.OpenFile(logpath, os.O_CREATE|os.O_WRONLY, 0666)
			if err == nil {
				l.Out = file
			} else {
				l.Info("Failed to log to file, using default stderr")
			}
		}

		instance = l
	})
	return instance
}

func MapLevelToLogrus(lvl string) logrus.Level {
	switch strings.ToLower(lvl) {
	case "panic":
		return logrus.PanicLevel
	case "fatal":
		return logrus.FatalLevel
	case "error":
		return logrus.ErrorLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "info":
		return logrus.InfoLevel
	case "debug":
		return logrus.DebugLevel
	}
	return logrus.InfoLevel
}
