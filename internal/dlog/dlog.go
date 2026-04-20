package dlog

import (
	"fmt"
	"log"
	"time"
)

var debugStart time.Time

// Debugging
const Debug = false

type LogTopic string

const (
	DClient   LogTopic = "CLNT"
	DCommit   LogTopic = "CMIT"
	DDrop     LogTopic = "DROP"
	DError    LogTopic = "ERRO"
	DInfo     LogTopic = "INFO"
	DLeader   LogTopic = "LEAD"
	DLog1     LogTopic = "LOG1"
	DLog2     LogTopic = "LOG2"
	DPersist  LogTopic = "PERS"
	DSnap     LogTopic = "SNAP"
	DHeartbit LogTopic = "HBIT"
	DTest     LogTopic = "TEST"
	DTimer    LogTopic = "TIMR"
	DTrace    LogTopic = "TRCE"
	DVote     LogTopic = "VOTE"
	DWarn     LogTopic = "WARN"
	DSuccess  LogTopic = "SUCC"
	DCtrler   LogTopic = "CTRLER"
	DGrpCl    LogTopic = "SGCL"
	DGrpSv    LogTopic = "SGSV"
)

func init() {
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Dlog(topic LogTopic, format string, a ...interface{}) {
	if !Debug {
		return
	}
	time := time.Since(debugStart).Microseconds()
	time /= 100
	prefix := fmt.Sprintf("%06d %v ", time, string(topic))
	format = prefix + format
	DPrintf(format, a...)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func ToTruncatedArrayItem[T any](item T) string {
	s := fmt.Sprintf("%v", item)
	if len(s) > 8 {
		return fmt.Sprintf("%v...", s[:8])
	} else {
		return fmt.Sprintf("%v", item)
	}
}

func ToTruncatedArrayString[T any](arr []T) string {
	truncatedStr := ""

	for itemIdx, item := range arr {
		if itemIdx == 0 {
			truncatedStr += "["
		}

		truncatedStr += ToTruncatedArrayItem(item)

		if itemIdx == len(arr)-1 {
			truncatedStr += "]"
		} else {
			truncatedStr += ","
		}
	}

	return truncatedStr
}
