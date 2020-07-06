package redis

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRedisStorage(t *testing.T) {
	s, err := NewRedisStore("192.168.10.16:6379", "111111", 14, 2, 5, "t_")
	assert.NoError(t, err)
	err = s.AppendLog("t_11", "{}")
	assert.NoError(t, err)
	looked, err := s.Lookup("t_11")
	assert.NoError(t, err)
	assert.Contains(t, looked, "{}")
	t.Log("looked:", looked)

	logId, err := s.LastLog("t_11")
	assert.NoError(t, err)
	assert.Equal(t, "{}", logId)

	logIds, err := s.LogIDs()
	assert.NoError(t, err)
	t.Log("logIds:", logIds)
}

func TestRedisStorage2(t *testing.T) {
	s, err := NewRedisStore("192.168.10.16:6379", "111111", 14, 2, 5, "t_")
	assert.NoError(t, err)
	err = s.AppendLog("t_11", "{1}")
	assert.NoError(t, err)
	err = s.AppendLog("t_12", "{1}")
	assert.NoError(t, err)
	err = s.AppendLog("t_12", "{2}")
	assert.NoError(t, err)

	logIds, err := s.LogIDs()
	assert.NoError(t, err)
	t.Log("logIds:", logIds)

	logId, err := s.LastLog("t_12")
	assert.NoError(t, err)
	assert.Equal(t, "{2}", logId)

	err = s.Cleanup("t_12")
	assert.NoError(t, err)

	logIds, err = s.LogIDs()
	assert.NoError(t, err)
	t.Log("logIds:", logIds)
}
