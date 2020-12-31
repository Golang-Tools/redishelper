package main

import (
	"context"
	"os"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	redishelper "github.com/Golang-Tools/redishelper"
)

const TEST_REDIS_URL = "redis://localhost:6379"

func regist() {
	redishelper.Proxy.Regist(func(cli redishelper.GoRedisV8Client) error {
		ctx := context.Background()
		log.Info("inited db")
		_, err := cli.Set(ctx, "teststring1", "ok", 10*time.Second).Result()
		if err != nil {
			log.Error("conn set error", log.Dict{
				"err": err,
			})
			return err
		}
		return nil
	})
	redishelper.Proxy.Regist(func(cli redishelper.GoRedisV8Client) error {
		ctx := context.Background()
		log.Info("inited db")
		_, err := cli.Set(ctx, "teststring2", "ok", 10*time.Second).Result()
		if err != nil {
			log.Error("conn set error", log.Dict{
				"err": err,
			})
			return err
		}
		return nil
	})
	redishelper.Proxy.Regist(func(cli redishelper.GoRedisV8Client) error {
		ctx := context.Background()
		log.Info("inited db")
		_, err := cli.Set(ctx, "teststring3", "ok", 10*time.Second).Result()
		if err != nil {
			log.Error("conn set error", log.Dict{
				"err": err,
			})
			return err
		}
		return nil
	})
}

func runget() {
	ctx := context.Background()
	res, err := redishelper.Proxy.Get(ctx, "teststring1").Result()
	if err != nil {
		log.Error("redis get error", log.Dict{
			"err": err,
		})
	} else {
		log.Info("redis get result", log.Dict{
			"res": res,
		})
	}
}
func main() {

	regist()
	err := redishelper.Proxy.InitFromURL(TEST_REDIS_URL)
	if err != nil {
		log.Error("proxy init error", log.Dict{
			"err": err,
		})
		os.Exit(1)
	}
	defer redishelper.Proxy.Close()
	runget()
}
