package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubscribe(t *testing.T) {

	t.Run("aze", func(t *testing.T) {
		conn := MockRedis().Connection()
		data := []byte("fuckoff")
		sub := conn.Subscribe("achannel")

		go conn.Send("achannel", data)

		assert.Equal(t, sub.GetData(), data, "data missmatched")
	})
}
