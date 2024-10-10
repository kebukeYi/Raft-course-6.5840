package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DError logTopic = "ERRO"
	DWarn  logTopic = "WARN" // level = 2
	DInfo  logTopic = "INFO" // level = 1
	DDebug logTopic = "DBUG" // level = 0

	// level = 1
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1" // sending log
	DLog2    logTopic = "LOG2" // receiving log
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DApply   logTopic = "APLY"
)

func getTopicLevel(topic logTopic) int {
	switch topic {
	case DError:
		return 3
	case DWarn:
		return 2
	case DInfo:
		return 1
	case DDebug:
		return 0
	default:
		return 1
	}
}
func getEnvLevel() int {
	v := os.Getenv("VERBOSE")
	level := getTopicLevel(DError) + 1
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var logStart time.Time
var logEnvLevel int

func init() {
	logStart = time.Now()
	logEnvLevel = getEnvLevel()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Log(peerId int, term int, userTopic logTopic, format string, a ...interface{}) {
	userTopicLevel := getTopicLevel(userTopic)
	if userTopicLevel >= logEnvLevel {
		timeStart := time.Since(logStart).Microseconds()
		timeStart /= 100
		//prefix := fmt.Sprintf("%06d T%04d %v S%d", timeStart, term, string(userTopicLevel), peerId)
		prefix := fmt.Sprintf("%06d T%04d %v S%d", timeStart, term, string(userTopic), peerId)
		format = prefix + format
		log.Printf(format, a...)
	}
}
