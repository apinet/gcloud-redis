package redis

import (
	"fmt"
	"reflect"
)

func RedisBatch(entities map[string]interface{}, conn RedisConnection) error {
	pipe := conn.Pipeline()

	entitiesCmds := map[string]map[int]interface{}{}

	for path, entity := range entities {
		entitiesCmds[path] = getJsonEntityCmds(path, entity, pipe)
	}

	if err := pipe.Exec(); err != nil {
		return err
	}

	for path, entityCmds := range entitiesCmds {
		updateJsonEntityWithCmds(entities[path], entityCmds)
	}

	return nil
}

func getJsonEntityCmds(path string, entity interface{}, pipe Pipeline) map[int]interface{} {
	v := reflect.ValueOf(entity).Elem()

	cmdsMap := map[int]interface{}{}
	numField := v.NumField()

	for i := 0; i < numField; i++ {
		field := v.Type().Field(i)

		jsonTag := field.Tag.Get("json")

		if len(jsonTag) > 0 {
			redisId := fmt.Sprintf("%s.%s", path, jsonTag)

			if field.Type.Kind() == reflect.Int {
				cmdsMap[i] = pipe.GetInt(redisId)
			}

			if field.Type.Kind() == reflect.String {
				cmdsMap[i] = pipe.GetString(redisId)
			}
		}
	}

	return cmdsMap
}

func updateJsonEntityWithCmds(entity interface{}, cmds map[int]interface{}) {
	v := reflect.ValueOf(entity).Elem()

	for i, cmd := range cmds {
		switch cmdT := cmd.(type) {
		case *GetIntCmd:
			v.Field(i).Set(reflect.ValueOf(cmdT.Value()))
		case *GetStringCmd:
			v.Field(i).Set(reflect.ValueOf(cmdT.Value()))
		}
	}
}
