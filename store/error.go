package store

import "errors"

var (
	//ErrDirNotExists raise when reading at an pg who does not have an dir
	ErrDirNotExists = errors.New("dir not exist yet")
)
