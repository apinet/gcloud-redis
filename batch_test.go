package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {

	t.Run("test exact match", func(t *testing.T) {
		conn := MockRedis().Connection()

		conn.SetString("doc1.field1", "11", 10)
		conn.SetInt("doc1.field2", 12, 10)
		conn.SetString("doc2.field1", "21", 10)
		conn.SetInt("doc2.field2", 22, 10)

		type Doc struct {
			Field1 string `json:"field1"`
			Field2 int    `json:"field2"`
			Field3 int
		}

		doc1 := Doc{}
		doc2 := Doc{}
		entities := map[string]interface{}{}
		entities["doc1"] = &doc1
		entities["doc2"] = &doc2

		err := RedisBatch(entities, conn)
		assert.Nil(t, err, "must succeed")

		assert.Equal(t, doc1.Field1, "11", "doc1.field1 error")
		assert.Equal(t, doc1.Field2, 12, "doc1.field2 error")
		assert.Equal(t, doc1.Field3, 0, "doc1.field3 error")

		assert.Equal(t, doc2.Field1, "21", "doc1.field1 error")
		assert.Equal(t, doc2.Field2, 22, "doc1.field2 error")
		assert.Equal(t, doc2.Field3, 0, "doc2.field3 error")
	})
}
