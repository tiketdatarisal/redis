package redis

import (
	"errors"
	jsoniter "github.com/json-iterator/go"
)

const (
	separator  = ":"
	patternAll = "*"

	networkTCP   = "tcp"
	errorWrapper = "%w: %v"

	cmdPing   = "PING"
	cmdGet    = "GET"
	cmdSet    = "SET"
	cmdSetEx  = "SETEX"
	cmdExits  = "EXISTS"
	cmdDel    = "DEL"
	cmdScan   = "SCAN"
	cmdMatch  = "MATCH"
	cmdUnLink = "UNLINK"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary

	ErrNotInitialized              = errors.New("redis was not initialized")
	ErrMarshalFailed               = errors.New("marshal binary to object failed")
	ErrPingFailed                  = errors.New("could not 'PING' redis")
	ErrGetFailed                   = errors.New("could not 'GET' specified key")
	ErrSetFailed                   = errors.New("could not 'SET' value to specified key")
	ErrSetExFailed                 = errors.New("could not 'SETEX' value to specified key")
	ErrKeyNotFound                 = errors.New("could not found specified key")
	ErrDelFailed                   = errors.New("could not 'DEL' specified key")
	ErrClearFailed                 = errors.New("could not clear redis")
	ErrScanFailed                  = errors.New("could not 'SCAN' keys")
	ErrInitializeGlobalRedisFailed = errors.New("could not initialize global redis client")

	// R a default Redis client singleton.
	R *Client = nil
)
