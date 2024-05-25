package main

import (
	"fmt"
	"time"
)

func main() {

	timer := time.NewTicker(time.Second)

	for {
		select {
		case <-timer.C:
			fmt.Println("Timer expired")
		default:
			fmt.Println("Waiting for timer to expire")
			time.Sleep(time.Millisecond * 200)
		}
	}

}
