package memory

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemStorage(t *testing.T) {
	s, err := NewMemStorage()
	assert.NoError(t, err)
	err = s.AppendLog("t_11", "{}")
	assert.NoError(t, err)
	looked, err := s.Lookup("t_11")
	assert.NoError(t, err)
	assert.Contains(t, looked, "{}")
}
