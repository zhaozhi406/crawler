package utils

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func HandleSignal(f func(), sig ...os.Signal) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sig...)
	s := <-ch
	f()

	log.Println("get signal:", s)
}

func HandleQuitSignal(f func()) {
	HandleSignal(f, syscall.SIGINT, syscall.SIGQUIT)
}
