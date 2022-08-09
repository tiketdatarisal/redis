package redis

import (
	"fmt"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"
	"strings"
	"time"
)

type Client struct {
	*redsync.Redsync
	pool *redis.Pool
}

// NewClient returns an initialized Redis client.
func NewClient(host string, user, password string) *Client {
	return NewClientFromPool(&redis.Pool{
		MaxActive: 10,
		MaxIdle:   5,
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			if user != "" && password != "" {
				return redis.Dial(networkTCP, host,
					redis.DialUsername(user),
					redis.DialPassword(password))
			} else if password != "" {
				return redis.Dial(networkTCP, host,
					redis.DialPassword(password))
			} else {
				return redis.Dial(networkTCP, host)
			}
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do(cmdPing)
			return err
		},
	})
}

// NewClientFromPool returns an initialized redis client that created from pool.
func NewClientFromPool(pool *redis.Pool) *Client {
	return &Client{pool: pool, Redsync: redsync.New(redigo.NewPool(pool))}
}

// Close will close Redis client.
func (e *Client) Close() {
	_ = e.pool.Close()
}

// Pool returns a reference to Redis pool. This used for accessing Redis framework directly.
func (e *Client) Pool() *redis.Pool {
	return e.pool
}

// Ping returns nil if ping is succeeded, otherwise return an error.
func (e *Client) Ping() error {
	if e == nil || e.pool == nil {
		return ErrNotInitialized
	}

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	if _, err := r.Do(cmdPing); err != nil {
		return fmt.Errorf(errorWrapper, ErrPingFailed, err)
	}

	return nil
}

// Get returns value from a key.
func (e *Client) Get(key string, namespace ...string) (interface{}, error) {
	if e == nil || e.pool == nil {
		return nil, ErrNotInitialized
	}

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, separator), ns)
	}

	if value, err := r.Do(cmdGet, ns); err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrGetFailed, err)
	} else {
		return value, nil

	}
}

// GetBool return a bool from a key.
func (e *Client) GetBool(key string, namespace ...string) (bool, error) {
	return redis.Bool(e.Get(key, namespace...))
}

// GetBytes returns byte slice from a key.
func (e *Client) GetBytes(key string, namespace ...string) ([]byte, error) {
	return redis.Bytes(e.Get(key, namespace...))
}

// GetInt return an int from a key.
func (e *Client) GetInt(key string, namespace ...string) (int, error) {
	return redis.Int(e.Get(key, namespace...))
}

// GetString returns string from a key.
func (e *Client) GetString(key string, namespace ...string) (string, error) {
	return redis.String(e.Get(key, namespace...))
}

// Set store a key and value to Redis.
func (e *Client) Set(key string, value interface{}, namespace ...string) error {
	if e == nil || e.pool == nil {
		return ErrNotInitialized
	}

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, separator), ns)
	}

	if _, err := r.Do(cmdSet, ns, value); err != nil {
		return fmt.Errorf(errorWrapper, ErrSetFailed, err)
	}

	return nil
}

// SetEx store a key and value with timeout to Redis.
func (e *Client) SetEx(key string, value interface{}, duration time.Duration, namespace ...string) error {
	if e == nil || e.pool == nil {
		return ErrNotInitialized
	}

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, separator), ns)
	}

	if _, err := r.Do(cmdSetEx, ns, int(duration/time.Second), value); err != nil {
		return fmt.Errorf(errorWrapper, ErrSetExFailed, err)
	}

	return nil
}

// Exists returns true if key is exists, otherwise false.
func (e *Client) Exists(key string, namespace ...string) (bool, error) {
	if e == nil || e.pool == nil {
		return false, ErrNotInitialized
	}

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, separator), ns)
	}

	if ok, err := redis.Bool(r.Do(cmdExits, ns)); err != nil {
		return false, fmt.Errorf(errorWrapper, ErrKeyNotFound, err)
	} else {
		return ok, nil
	}
}

// Delete erase a key and value from Redis.
func (e *Client) Delete(key string, namespace ...string) error {
	if e == nil || e.pool == nil {
		return ErrNotInitialized
	}

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, separator), ns)
	}

	if _, err := r.Do(cmdDel, ns); err != nil {
		return fmt.Errorf(errorWrapper, ErrDelFailed, err)
	}

	return nil
}

// Clear remove keys with pattern. Note this is dangerous operation please do be careful.
func (e *Client) Clear(pattern string, namespace ...string) error {
	if e == nil || e.pool == nil {
		return ErrNotInitialized
	}

	var (
		cursor int64
		keys   []interface{}
	)

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	ns := pattern
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, separator), ns)
	}

	for {
		values, err := redis.Values(r.Do(cmdScan, cursor, cmdMatch, fmt.Sprintf(ns)))
		if err != nil {
			return fmt.Errorf(errorWrapper, ErrClearFailed, err)
		}

		values, err = redis.Scan(values, &cursor, &keys)
		if err != nil {
			return fmt.Errorf(errorWrapper, ErrClearFailed, err)
		}

		if len(keys) > 0 {
			_, err = r.Do(cmdUnLink, keys...)
			if err != nil {
				return fmt.Errorf(errorWrapper, ErrClearFailed, err)
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

// ClearAll remove all keys. Note this is dangerous operation please do be careful.
func (e *Client) ClearAll(namespace ...string) error {
	return e.Clear(patternAll, namespace...)
}

// GetKeys get slice of keys that available in this Redis.
func (e *Client) GetKeys(pattern string, namespace ...string) ([]string, error) {
	if e == nil || e.pool == nil {
		return nil, ErrNotInitialized
	}

	var (
		cursor int64
		items  []string
	)

	r := e.pool.Get()
	defer func() { _ = r.Close() }()

	ns := pattern
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, separator), ns)
	}

	var keys []string
	for {
		values, err := redis.Values(r.Do(cmdScan, cursor, cmdMatch, fmt.Sprintf(ns)))
		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrScanFailed, err)
		}

		values, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrScanFailed, err)
		}

		keys = append(keys, items...)
		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

// GetAllKeys get all keys that available in this Redis.
func (e *Client) GetAllKeys(namespace ...string) ([]string, error) {
	return e.GetKeys(patternAll, namespace...)
}

// SetObject set object.
func (e *Client) SetObject(key string, value interface{}, namespace ...string) error {
	raw, _ := json.Marshal(value)
	return e.Set(key, raw, namespace...)
}

// SetObjectEx set object with expiration.
func (e *Client) SetObjectEx(key string, value interface{}, duration time.Duration, namespace ...string) error {
	raw, _ := json.Marshal(value)
	return e.SetEx(key, raw, duration, namespace...)
}

// GetObject return object.
func (e *Client) GetObject(key string, obj interface{}, namespace ...string) error {
	raw, err := e.GetBytes(key, namespace...)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(raw, obj); err != nil {
		return ErrMarshalFailed
	}

	return nil
}
