package redis

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var ErrMustBeAPointerOfStruct = errors.New("must be a pointer of struct")

func RedisSnap(id string, with interface{}, ttl int, conn RedisConnection) error {
	if with == nil {
		return ErrMustBeAPointerOfStruct
	}

	v := reflect.ValueOf(with)

	if v.Kind() != reflect.Ptr {
		return ErrMustBeAPointerOfStruct
	}

	v = v.Elem()

	if v.Kind() != reflect.Struct {
		return ErrMustBeAPointerOfStruct
	}

	numField := v.NumField()
	cmdsMap := map[int]interface{}{}

	pipe := conn.Pipeline()

	for i := 0; i < numField; i++ {
		field := v.Type().Field(i)

		jsonTags := strings.Split(field.Tag.Get("json"), ",")
		redisTag := field.Tag.Get("redis")

		if len(jsonTags) > 0 && len(redisTag) > 0 {
			redisId := fmt.Sprintf("%s.%s", id, jsonTags[0])

			if field.Type.Kind() == reflect.Int {
				switch redisTag {
				case "set":
					if v.Field(i).Int() != 0 {
						pipe.SetInt(redisId, int(v.Field(i).Int()), ttl)
					} else {
						cmdsMap[i] = pipe.GetInt(redisId)
					}

				case "get":
					cmdsMap[i] = pipe.GetInt(redisId)

				case "inc":
					cmdsMap[i] = pipe.IncrBy(redisId, int(v.Field(i).Int()))
					pipe.SetExpire(redisId, ttl)
				}
			}

			if field.Type.Kind() == reflect.String {
				switch redisTag {
				case "set":
					if len(v.Field(i).String()) > 0 {
						pipe.SetString(redisId, v.Field(i).String(), ttl)
					} else {
						cmdsMap[i] = pipe.GetString(redisId)
					}
				case "get":
					cmdsMap[i] = pipe.GetString(redisId)
				}
			}
		}
	}

	if err := pipe.Exec(); err != nil {
		return err
	}

	for i, cmd := range cmdsMap {
		switch cmdT := cmd.(type) {
		case *IncrByCmd:
			v.Field(i).Set(reflect.ValueOf(cmdT.Value()))
		case *GetIntCmd:
			v.Field(i).Set(reflect.ValueOf(cmdT.Value()))
		case *GetStringCmd:
			v.Field(i).Set(reflect.ValueOf(cmdT.Value()))
		}
	}

	return nil
}
