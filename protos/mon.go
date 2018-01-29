package protos

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func (m *PgMaps) GetPgParentId(pgId string) (string, error) {
	array := strings.Split(pgId, ".")
	pool, _ := strconv.Atoi(array[0])
	id, _ := strconv.Atoi(array[1])
	if pgmap, ok := m.Pgmaps[int32(pool)]; ok {
		if pg, ok := pgmap.Pgmap[int32(id)]; ok {
			parentId := fmt.Sprintf("%d.%d", pool, pg.ParentId)
			return parentId, nil
		}
	}
	return "", errors.New("pg not exist")
}
