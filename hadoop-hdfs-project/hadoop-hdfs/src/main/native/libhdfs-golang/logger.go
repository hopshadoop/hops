package main

import (
	"fmt"
	"os"
	"strconv"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log_file_io_Writer *os.File
var logger *zap.Logger

//------------------------------------------------------------------------------

func DEBUG(msg string, args ...interface{}) {
	new_msg := fmt.Sprintf(msg, args...)
	if logger != nil {
		logger.Debug(new_msg)
	}
}

//------------------------------------------------------------------------------

func INFO(msg string, args ...interface{}) {
	new_msg := fmt.Sprintf(msg, args...)
	if logger != nil {
		logger.Info(new_msg)
	}
}

//------------------------------------------------------------------------------

func ERROR(msg string, args ...interface{}) {
	new_msg := fmt.Sprintf(msg, args...)
	if logger != nil {
		logger.Error(new_msg)
	}
}

//------------------------------------------------------------------------------

func init_logger() {
	enable_logger_str := os.Getenv(LIBHDFS_ENABLE_LOG)

	if enable_logger_str == "" {
		return
	}

	boolValue, err := strconv.ParseBool(enable_logger_str)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Bad value for %s. Error %v\n", LIBHDFS_ENABLE_LOG, err)
		return
	}

	if boolValue {
		if logger == nil {

			log_file := os.Getenv(LIBHDFS_LOG_FILE)

			if log_file != "" {
				log_file_io_Writer, err = os.OpenFile(log_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					fmt.Fprintf(os.Stderr, "error opening log file: %v\n", err)
					return
				}
			}

			// init zap logger
			encoderCfg := zap.NewProductionEncoderConfig()
			encoderCfg.TimeKey = "time"
			encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

			var syncer zapcore.WriteSyncer
			if log_file != "" {
				syncer = zapcore.AddSync(log_file_io_Writer)
			} else {
				syncer = zapcore.Lock(os.Stdout)
			}

			logger = zap.New(zapcore.NewCore(
				zapcore.NewJSONEncoder(encoderCfg),
				syncer,
				zap.DebugLevel,
			))
		}
	}
}

//------------------------------------------------------------------------------

func shutdown_logger() {
	if logger != nil {
		logger.Sync()
		logger = nil
		log_file_io_Writer.Close()
	}
}
