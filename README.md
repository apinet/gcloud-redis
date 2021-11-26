# gcloud-redis

Redis (gcloud) wrapper with mocking capabilities for golang cloud functions.
This package is based on redigo.

## usage

### initiate redis

```go
import (
  redis "github.com/apinet/gcloud-redis"
)

maxIdle := 10
r := redis.NewRedis("ip:port", maxIdle)
```

### key operations

```go
r.SetString("key1", "a value")
r.Delete("key1", "key2")
```

### Pipeline operations

```go
pipe := r.Pipeline()

pipe.SetString("key1", "value")
incr := pipe.IncrBy("key2", 2)

if err := pipe.Exec() ; err != nil {
  return err
}

// the incremented value after exec
incr.Value()

```
