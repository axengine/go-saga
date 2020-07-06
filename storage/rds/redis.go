package rds

import (
	"github.com/gomodule/redigo/redis"
	"strings"
	"time"
)

type RedisStore struct {
	pool      *redis.Pool
	logPrefix string
}

func NewRedisStore(dial, password string, db, maxIdle, maxActive int, logPrefix string) (*RedisStore, error) {
	if maxIdle == 0 {
		maxIdle = 2
	}
	if maxActive == 0 {
		maxActive = 10
	}

	pool := &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: 120 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", dial)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", db); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return &RedisStore{
		pool:      pool,
		logPrefix: logPrefix,
	}, nil
}

// AppendLog appends log data into log under given logID
func (p *RedisStore) AppendLog(logID string, data string) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := redis.Int64(conn.Do("RPUSH", logID, data))
	return err
}

// Lookup uses to lookup all log under given logID
func (p *RedisStore) Lookup(logID string) ([]string, error) {
	conn := p.pool.Get()
	defer conn.Close()
	replys, err := redis.Strings(conn.Do("LRANGE", logID, 0, -1))
	return replys, err
}

// Close use to close storage and release resources
func (p *RedisStore) Close() error {
	return p.pool.Close()
}

// LogIDs returns exists logID
func (p *RedisStore) LogIDs() ([]string, error) {
	conn := p.pool.Get()
	defer conn.Close()
	keys, err := redis.Strings(conn.Do("KEYS", "*"))
	sagaTopics := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, p.logPrefix) {
			sagaTopics = append(sagaTopics, key)
		}
	}

	return sagaTopics, err
}

// Cleanup cleans up all log data in logID
func (p *RedisStore) Cleanup(logID string) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", logID)
	return err
}

// LastLog fetch last log entry with given logID
func (p *RedisStore) LastLog(logID string) (string, error) {
	conn := p.pool.Get()
	defer conn.Close()
	replys, err := redis.Strings(conn.Do("LRANGE", logID, -1, -1))
	if len(replys) == 0 {
		return "", err
	}
	return replys[0], err
}
