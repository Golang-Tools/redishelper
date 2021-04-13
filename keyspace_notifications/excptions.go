package keyspace_notifications

import (
	"errors"
)

//ErrNoitificationsAlreadyListened keyspace-notification已经被监听了
var ErrNoitificationsAlreadyListened = errors.New("keyspace-notification已经被监听了")

//ErrNoitificationsNotListeningYet keyspace-notification未被监听
var ErrNoitificationsNotListeningYet = errors.New("keyspace-notification未被监听")
