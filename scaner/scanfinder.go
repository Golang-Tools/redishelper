package scaner

import (
	"context"

	"github.com/Golang-Tools/optparams"
	redis "github.com/go-redis/redis/v8"
)

type Scanner struct {
	cli redis.UniversalClient
	opt Options
}

type ScannerINterface interface {
	Find(ctx context.Context, partten string) ([]string, error)
	FindOne(ctx context.Context, partten string) (string, error)
}

func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) *Scanner {
	s := new(Scanner)
	s.opt = Options{
		StepSize: 50,
	}
	optparams.GetOption(&s.opt, opts...)
	s.cli = cli
	return s
}

func (s *Scanner) findIter(ctx context.Context, partten string, cursor uint64, stepsize int64, resultkeys []string) ([]string, error) {
	keys, newcursor, err := s.cli.Scan(ctx, cursor, partten, stepsize).Result()
	if err != nil {
		return resultkeys, err
	}
	resultkeys = append(resultkeys, keys...)
	if newcursor != 0 {
		return s.findIter(ctx, partten, newcursor, stepsize, resultkeys)
	}
	return resultkeys, nil
}

//Find 查询满足匹配的所有key
func (s *Scanner) Find(ctx context.Context, partten string) ([]string, error) {
	resultkeys := []string{}
	return s.findIter(ctx, partten, 0, s.opt.StepSize, resultkeys)
}

func (s *Scanner) findOneIter(ctx context.Context, partten string, cursor uint64, stepsize int64) (string, error) {
	keys, newcursor, err := s.cli.Scan(ctx, cursor, partten, stepsize).Result()
	if err != nil {
		return "", err
	}
	if len(keys) > 0 {
		return keys[0], nil
	}
	return s.findOneIter(ctx, partten, newcursor, stepsize)
}

//FindOne 查询满足匹配的一个key
func (s *Scanner) FindOne(ctx context.Context, partten string) (string, error) {
	return s.findOneIter(ctx, partten, 0, s.opt.StepSize)
}
