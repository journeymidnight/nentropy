package osd

import "errors"

//ErrNoSuchPG : pg not exist
var ErrNoSuchPG = errors.New("no such pg")

//ErrFailedOpenBadgerStore : failed to open badger store
var ErrFailedOpenBadgerStore = errors.New("failed to open badger store")
