package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerGameLog(gl routing.GameLog) pubsub.Acktype {
	if err := gamelogic.WriteLog(gl); err != nil {
		fmt.Printf("error writing log: %s\n", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
