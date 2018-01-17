package osd

import "errors"

//ErrNoSuchPG : pg not exist
var ErrNoSuchPG = errors.New("no such pg")

//ErrPGAlreadyExists : pg already exist
var ErrPGAlreadyExists = errors.New("pg already exists")

//ErrFailedOpenBadgerStore : failed to open badger store
var ErrFailedOpenBadgerStore = errors.New("failed to open badger store")

//ErrFailedSettingKey : failed setting key to badger
var ErrFailedSettingKey = errors.New("failed setting key to badger")

//ErrFailedRemovingKey : failed removing key to badger
var ErrFailedRemovingKey = errors.New("failed removing key to badger")

//ErrNoValueForKey : no value for this key
var ErrNoValueForKey = errors.New("no value for this key")
