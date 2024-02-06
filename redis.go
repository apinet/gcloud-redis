package redis

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/gomodule/redigo/redis"
)

func NewRedis(address string, maxIdle int) Redis {
	pool := &redis.Pool{
		MaxIdle: maxIdle,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", address)
			if err != nil {
				return nil, fmt.Errorf("redis.Dial: %v", err)
			}
			return c, err
		},
	}

	return &RedisImpl{
		pool: pool,
	}
}

type Redis interface {
	Connection() RedisConnection
}

type RedisConnection interface {
	Exists(key string) (bool, error)
	SetExpire(key string, ttl int) error
	GetExpire(key string) (int, error)
	Delete(keys ...string) (int, error)

	GetString(key string) (string, bool, error)
	SetString(key string, src string, ttl int) error

	GetInt(key string) (int, bool, error)
	SetInt(key string, value int, ttl int) error

	IncrBy(key string, by int) (int, error)

	Pipeline() Pipeline

	Close()
}

type RedisImpl struct {
	pool *redis.Pool
}

func (r *RedisImpl) Connection() RedisConnection {

	return &RedisConnectionImpl{
		conn: r.pool.Get(),
	}
}

type RedisConnectionImpl struct {
	conn redis.Conn
}

func (c *RedisConnectionImpl) IncrBy(key string, by int) (int, error) {
	return redis.Int(c.conn.Do("INCRBY", key, by))
}

func (c *RedisConnectionImpl) Exists(key string) (bool, error) {
	value, err := redis.Int(c.conn.Do("EXISTS", key))

	if err != nil {
		return false, err
	}

	return value > 0, nil
}

func (c *RedisConnectionImpl) GetString(key string) (string, bool, error) {
	return getString(c.conn.Do("GET", key))
}

func (c *RedisConnectionImpl) SetString(key string, value string, ttl int) error {
	_, err := c.conn.Do("SETEX", key, ttl, value)
	return err
}

func (c *RedisConnectionImpl) SetExpire(key string, ttl int) error {
	_, err := c.conn.Do("EXPIRE", key, ttl)
	return err
}

func (c *RedisConnectionImpl) GetExpire(key string) (int, error) {
	return getTTL(c.conn.Do("TTL", key))
}

func (c *RedisConnectionImpl) SetInt(key string, src int, ttl int) error {
	_, err := c.conn.Do("SETEX", key, ttl, src)
	return err
}

func (c *RedisConnectionImpl) GetInt(key string) (int, bool, error) {
	return getInt(c.conn.Do("GET", key))
}

func (c *RedisConnectionImpl) Pipeline() Pipeline {
	return &PipelineImpl{
		conn: c.conn,
		cmds: make([]interface{}, 0, 30),
	}
}

func (c *RedisConnectionImpl) Delete(keys ...string) (int, error) {
	iKeys := make([]interface{}, 0, len(keys))

	for _, key := range keys {
		iKeys = append(iKeys, key)
	}

	return redis.Int(c.conn.Do("DEL", iKeys...))
}

func (c *RedisConnectionImpl) Close() {
	c.conn.Close()
}

type Pipeline interface {
	GetInt(key string) *GetIntCmd
	SetInt(key string, value int, ttl int)

	SetExpire(key string, ttl int)
	GetExpire(key string) *GetExpireCmd

	IncrBy(key string, by int) *IncrByCmd

	GetString(key string) *GetStringCmd
	SetString(key string, value string, ttl int)

	Delete(key string) *DeleteCmd
	Exec() error
}

type PipelineImpl struct {
	cmds []interface{}
	conn redis.Conn
}

func (p *PipelineImpl) GetInt(key string) *GetIntCmd {
	cmd := GetIntCmd{
		key:   key,
		found: false,
		value: 0,
	}

	p.cmds = append(p.cmds, &cmd)
	return &cmd
}

func (p *PipelineImpl) SetInt(key string, value int, ttl int) {
	cmd := SetIntCmd{
		key:   key,
		value: value,
		ttl:   ttl,
	}

	p.cmds = append(p.cmds, &cmd)
}

func (p *PipelineImpl) SetExpire(key string, ttl int) {
	cmd := SetExpireCmd{
		key:   key,
		value: ttl,
	}

	p.cmds = append(p.cmds, &cmd)
}

func (p *PipelineImpl) GetExpire(key string) *GetExpireCmd {
	cmd := GetExpireCmd{
		key: key,
	}

	p.cmds = append(p.cmds, &cmd)
	return &cmd
}

func (p *PipelineImpl) IncrBy(key string, by int) *IncrByCmd {
	cmd := IncrByCmd{
		key:   key,
		by:    by,
		value: 0,
	}

	p.cmds = append(p.cmds, &cmd)
	return &cmd
}

func (p *PipelineImpl) GetString(key string) *GetStringCmd {
	cmd := GetStringCmd{
		key:   key,
		found: false,
		value: "",
	}

	p.cmds = append(p.cmds, &cmd)
	return &cmd
}

func (p *PipelineImpl) SetString(key string, value string, ttl int) {
	cmd := SetStringCmd{
		key:   key,
		value: value,
		ttl:   ttl,
	}

	p.cmds = append(p.cmds, &cmd)
}

func (p *PipelineImpl) Delete(key string) *DeleteCmd {
	cmd := DeleteCmd{
		key:   key,
		found: false,
	}

	p.cmds = append(p.cmds, &cmd)
	return &cmd
}

// Exec send and receive registered commands and set corresponding values
func (p *PipelineImpl) Exec() error {
	if err := sendCmds(p.conn, p.cmds); err != nil {
		return err
	}

	if err := p.conn.Flush(); err != nil {
		return err
	}

	if err := receiveCmds(p.conn, p.cmds); err != nil {
		return err
	}

	return nil
}

// each *Cmd must have input and output field
type GetStringCmd struct {
	key   string
	value string
	found bool
}

func (g *GetStringCmd) Value() string {
	return g.value
}

func (g *GetStringCmd) Found() bool {
	return g.found
}

type GetIntCmd struct {
	key   string
	value int
	found bool
}

func (g *GetIntCmd) Value() int {
	return g.value
}

func (g *GetIntCmd) Found() bool {
	return g.found
}

type GetExpireCmd struct {
	key   string
	value int
}

func (g *GetExpireCmd) Value() int {
	return g.value
}

type DeleteCmd struct {
	key   string
	found bool
}

func (d *DeleteCmd) Found() bool {
	return d.found
}

type SetStringCmd struct {
	key   string
	ttl   int
	value string
}

type SetIntCmd struct {
	key   string
	ttl   int
	value int
}

type SetExpireCmd struct {
	key   string
	value int
}

type IncrByCmd struct {
	key   string
	by    int
	value int
}

func (i *IncrByCmd) Value() int {
	return i.value
}

func sendCmds(conn redis.Conn, cmds []interface{}) error {
	for _, cmd := range cmds {
		switch cmd := cmd.(type) {
		case *GetIntCmd:
			if err := conn.Send("GET", cmd.key); err != nil {
				return err
			}

		case *GetExpireCmd:
			if err := conn.Send("TTL", cmd.key); err != nil {
				return err
			}
		case *SetIntCmd:
			if err := conn.Send("SETEX", cmd.key, cmd.ttl, cmd.value); err != nil {
				return err
			}

		case *IncrByCmd:
			if err := conn.Send("INCRBY", cmd.key, cmd.by); err != nil {
				return err
			}

		case *GetStringCmd:
			if err := conn.Send("GET", cmd.key); err != nil {
				return err
			}

		case *SetStringCmd:
			if err := conn.Send("SETEX", cmd.key, cmd.ttl, cmd.value); err != nil {
				return err
			}

		case *SetExpireCmd:
			if err := conn.Send("EXPIRE", cmd.key, cmd.value); err != nil {
				return err
			}
		case *DeleteCmd:
			if err := conn.Send("DEL", cmd.key); err != nil {
				return err
			}

		default:
			return errors.New("unsupported command")
		}
	}

	return nil
}

func receiveCmds(conn redis.Conn, cmds []interface{}) error {
	for _, cmd := range cmds {
		switch cmd := cmd.(type) {
		case *GetIntCmd:
			value, found, err := getInt(conn.Receive())

			if err != nil {
				return err
			}

			cmd.found = found
			cmd.value = value

		case *SetIntCmd:
			if _, err := conn.Receive(); err != nil {
				return err
			}

		case *IncrByCmd:
			value, found, err := getInt(conn.Receive())

			if err != nil || !found {
				return err
			}

			cmd.value = value

		case *GetStringCmd:
			value, found, err := getString(conn.Receive())

			if err != nil {
				return err
			}

			cmd.found = found
			cmd.value = value

		case *SetStringCmd:
			if _, err := conn.Receive(); err != nil {
				return err
			}

		case *SetExpireCmd:
			if _, err := conn.Receive(); err != nil {
				return err
			}

		case *GetExpireCmd:
			if _, err := getTTL(conn.Receive()); err != nil {
				return err
			}

		case *DeleteCmd:
			num, err := redis.Int(conn.Receive())
			if err != nil {
				return err
			}

			if num > 0 {
				cmd.found = true
			}
			cmd.found = false

		default:
			return errors.New("unsupported command")
		}
	}

	return nil
}

func getInt(value interface{}, err error) (int, bool, error) {
	intVal, err := redis.Int(value, err)

	if err == redis.ErrNil {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	return intVal, true, nil
}

func getTTL(value interface{}, err error) (int, error) {
	intVal, err := redis.Int(value, err)

	if err != nil {
		return 0, err
	}

	if intVal < 0 {
		return 0, err
	}

	return intVal, nil
}

func getString(value interface{}, err error) (string, bool, error) {
	stringVal, err := redis.String(value, err)

	if err == redis.ErrNil {
		return "", false, nil
	}

	if err != nil {
		return "", false, err
	}

	return stringVal, true, nil
}

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

		jsonTag := field.Tag.Get("json")
		redisTag := field.Tag.Get("redis")

		if len(jsonTag) > 0 && len(redisTag) > 0 {
			redisId := fmt.Sprintf("%s.%s", id, jsonTag)

			if field.Type.Kind() == reflect.Int {
				switch redisTag {
				case "set":
					if v.Field(i).Int() != 0 {
						pipe.SetInt(redisId, int(v.Field(i).Int()), ttl)
					}
				case "get":
					cmdsMap[i] = pipe.GetInt(redisId)
				case "inc":
					if v.Field(i).Int() != 0 {
						cmdsMap[i] = pipe.IncrBy(redisId, int(v.Field(i).Int()))
						pipe.SetExpire(redisId, ttl)
					}
				}
			}

			if field.Type.Kind() == reflect.String {
				switch redisTag {
				case "set":
					if len(v.Field(i).String()) > 0 {
						pipe.SetString(redisId, v.Field(i).String(), ttl)
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
