package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(100-50) + 50
}
