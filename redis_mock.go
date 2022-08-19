package redis

import (
	"errors"
)

func MockRedis() *RedisMock {

	return &RedisMock{
		db: make(map[string]*RedisMockObject),

		failsOnGet: make(map[string]bool),
		failsOnSet: make(map[string]bool),
		failsOnDel: make(map[string]bool),

		now: 0,
	}
}

type RedisMockObject struct {
	data      interface{}
	expiresAt int
}

type RedisMock struct {
	db map[string]*RedisMockObject

	failsOnGet map[string]bool
	failsOnSet map[string]bool
	failsOnDel map[string]bool
	now        int // in sec

	openedConnections int
}

func (r *RedisMock) Connection() RedisConnection {
	r.openedConnections++

	return &RedisConnectionMock{
		redis: r,
	}
}

func (r *RedisMock) FailsOnGet(key string, fails bool) *RedisMock {
	r.failsOnGet[key] = fails
	return r
}

func (r *RedisMock) FailsOnSet(key string, fails bool) *RedisMock {
	r.failsOnSet[key] = fails
	return r
}

func (r *RedisMock) FailsOnDel(key string, fails bool) *RedisMock {
	r.failsOnDel[key] = fails
	return r
}

func (r *RedisMock) SetNow(now int) *RedisMock {
	r.now = now
	return r
}

func (r *RedisMock) GetNumKeys() int {
	num := 0
	for _, obj := range r.db {
		if obj.expiresAt == 0 || obj.expiresAt > r.now {
			num++
		}
	}

	return num
}

func (r *RedisMock) With(key string, value interface{}, ttl int) *RedisMock {
	r.set(key, value, ttl)
	return r
}

func (r *RedisMock) GetOpenedConnections() int {
	return r.openedConnections
}

func (r *RedisMock) get(key string) (interface{}, bool, error) {
	if r.failsOnGet[key] {
		return nil, false, errors.New("fails on get")
	}

	redisMockObject := r.db[key]

	if redisMockObject == nil || redisMockObject.expiresAt != 0 && redisMockObject.expiresAt <= r.now {
		return nil, false, nil
	}

	return redisMockObject.data, true, nil
}

func (r *RedisMock) set(key string, value interface{}, ttl int) error {
	if r.failsOnSet[key] {
		return errors.New("fails on set")
	}

	expiresAt := 0

	if ttl > 0 {
		expiresAt = r.now + ttl
	}

	r.db[key] = &RedisMockObject{
		data:      value,
		expiresAt: expiresAt,
	}
	return nil
}

func (r *RedisMock) GetExpireTime(key string) (int, bool) {
	redisMockObject := r.db[key]

	if redisMockObject == nil {
		return 0, false
	}

	return redisMockObject.expiresAt, true
}

type RedisConnectionMock struct {
	redis *RedisMock
}

func (c *RedisConnectionMock) IncrBy(key string, by int) (int, error) {
	value, found, err := c.redis.get(key)

	if err != nil {
		return 0, err
	}

	if found {
		v := value.(int) + by
		err = c.redis.set(key, v, 0)
		return v, err
	}

	err = c.redis.set(key, by, 0)
	return by, err
}

func (c *RedisConnectionMock) Exists(key string) (bool, error) {
	_, found, err := c.redis.get(key)
	return found, err
}

func (c *RedisConnectionMock) SetInt(key string, src int, ttl int) error {
	return c.redis.set(key, src, ttl)
}

func (c *RedisConnectionMock) SetExpire(key string, ttl int) error {
	value, found, err := c.redis.get(key)

	if err != nil {
		return err
	}

	if !found {
		return errors.New("can't expire unknown key")
	}

	return c.redis.set(key, value, ttl)
}

func (c *RedisConnectionMock) GetInt(key string) (int, bool, error) {
	value, found, err := c.redis.get(key)

	if err != nil {
		return 0, false, err
	}

	if !found {
		return 0, false, nil
	}

	v := value.(int)
	return v, true, err
}

func (c *RedisConnectionMock) SetString(key string, value string, ttl int) error {
	return c.redis.set(key, value, ttl)
}

func (c *RedisConnectionMock) GetString(key string) (string, bool, error) {
	value, found, err := c.redis.get(key)

	if err != nil {
		return "", false, err
	}

	if !found {
		return "", false, nil
	}

	v := value.(string)
	return v, true, err
}

func (c *RedisConnectionMock) Close() {
	c.redis.openedConnections--
}

func (c *RedisConnectionMock) Delete(keys ...string) error {
	for _, key := range keys {
		if c.redis.failsOnDel[key] {
			return errors.New("fails on del")
		}

		delete(c.redis.db, key)
	}

	return nil
}

func (c *RedisConnectionMock) Pipeline() Pipeline {
	return &PipelineMock{
		conn: c,
		cmds: make([]interface{}, 0, 30),
	}
}

type PipelineMock struct {
	conn *RedisConnectionMock
	cmds []interface{}
}

func (p *PipelineMock) GetInt(key string) *GetIntCmd {
	cmd := GetIntCmd{key: key}
	p.cmds = append(p.cmds, &cmd)

	return &cmd
}

func (p *PipelineMock) SetInt(key string, value int, ttl int) {
	cmd := SetIntCmd{key: key, value: value, ttl: ttl}
	p.cmds = append(p.cmds, &cmd)
}

func (p *PipelineMock) SetExpire(key string, ttl int) {
	cmd := SetExpireCmd{key: key, value: ttl}
	p.cmds = append(p.cmds, &cmd)
}

func (p *PipelineMock) IncrBy(key string, by int) *IncrByCmd {
	cmd := IncrByCmd{key: key, by: by}
	p.cmds = append(p.cmds, &cmd)

	return &cmd
}

func (p *PipelineMock) GetString(key string) *GetStringCmd {
	cmd := GetStringCmd{key: key}
	p.cmds = append(p.cmds, &cmd)

	return &cmd
}

func (p *PipelineMock) SetString(key string, value string, ttl int) {
	cmd := SetStringCmd{key: key, value: value, ttl: ttl}
	p.cmds = append(p.cmds, &cmd)
}

func (p *PipelineMock) Delete(key string) {
	cmd := DeleteCmd{key: key}
	p.cmds = append(p.cmds, &cmd)
}
func (p *PipelineMock) Exec() error {
	for _, cmd := range p.cmds {
		switch cmd := cmd.(type) {
		case *GetIntCmd:
			value, found, err := p.conn.GetInt(cmd.key)

			if err != nil {
				return err
			}

			cmd.value = value
			cmd.found = found

		case *SetIntCmd:
			if err := p.conn.SetInt(cmd.key, cmd.value, cmd.ttl); err != nil {
				return err
			}

		case *SetExpireCmd:
			if err := p.conn.SetExpire(cmd.key, cmd.value); err != nil {
				return err
			}

		case *IncrByCmd:
			value, err := p.conn.IncrBy(cmd.key, cmd.by)

			if err != nil {
				return err
			}

			cmd.value = value

		case *GetStringCmd:
			value, found, err := p.conn.GetString(cmd.key)

			if err != nil {
				return err
			}

			cmd.value = value
			cmd.found = found

		case *SetStringCmd:
			if err := p.conn.SetString(cmd.key, cmd.value, cmd.ttl); err != nil {
				return err
			}

		case *DeleteCmd:
			if err := p.conn.Delete(cmd.key); err != nil {
				return err
			}

		default:
			return errors.New("unsupported command")
		}

	}
	return nil
}
