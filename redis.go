package redis

import "fmt"

// SetDefaultRedis set default Redis with configurations.
func SetDefaultRedis(host string, user, password string) {
	R = NewClient(host, user, password)
	if R == nil {
		return
	}

	if err := R.Ping(); err != nil {
		panic(fmt.Errorf(errorWrapper, ErrInitializeGlobalRedisFailed, err))
	}
}

// CloseDefaultRedis close and cleanup default Redis.
func CloseDefaultRedis() {
	if R != nil {
		R.Close()
	}
}
