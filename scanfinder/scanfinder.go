package scanfinder

import (
	"context"

	redis "github.com/go-redis/redis/v8"
)

func findIter(ctx context.Context, cli redis.UniversalClient, partten string, cursor uint64, stepsize int64, resultkeys []string) ([]string, error) {
	keys, newcursor, err := cli.Scan(ctx, cursor, partten, stepsize).Result()
	if err != nil {
		return resultkeys, err
	}
	resultkeys = append(resultkeys, keys...)
	if newcursor != 0 {
		return findIter(ctx, cli, partten, newcursor, stepsize, resultkeys)
	}
	return resultkeys, nil
}

//Find 查询满足匹配的所有key
func Find(ctx context.Context, cli redis.UniversalClient, partten string, opts ...Option) ([]string, error) {
	resultkeys := []string{}
	defaultopt := Options{
		StepSize: 50,
	}
	for _, opt := range opts {
		opt.Apply(&defaultopt)
	}
	return findIter(ctx, cli, partten, 0, defaultopt.StepSize, resultkeys)
}

func findOneIter(ctx context.Context, cli redis.UniversalClient, partten string, cursor uint64, stepsize int64) (string, error) {
	keys, newcursor, err := cli.Scan(ctx, cursor, partten, stepsize).Result()
	if err != nil {
		return "", err
	}
	if len(keys) > 0 {
		return keys[0], nil
	}
	return findOneIter(ctx, cli, partten, newcursor, stepsize)
}

//FindOne 查询满足匹配的一个key
func FindOne(ctx context.Context, cli redis.UniversalClient, partten string, opts ...Option) (string, error) {
	defaultopt := Options{
		StepSize: 50,
	}
	for _, opt := range opts {
		opt.Apply(&defaultopt)
	}
	return findOneIter(ctx, cli, partten, 0, defaultopt.StepSize)
}
