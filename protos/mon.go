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

func (m *PoolMap) GetPoolSize(pgId string) (int32, error) {
	array := strings.Split(pgId, ".")
	pool, _ := strconv.Atoi(array[0])
	if poolMap, ok := m.Pools[int32(pool)]; ok {
		return poolMap.Size_, nil
	}
	return 0, errors.New("pool not exist")
}

func (m *PgMaps) GetRepLenInPgMap(pgId string) (int32, error) {
	array := strings.Split(pgId, ".")
	pool, _ := strconv.Atoi(array[0])
	id, _ := strconv.Atoi(array[1])
	if pgmap, ok := m.Pgmaps[int32(pool)]; ok {
		if pg, ok := pgmap.Pgmap[int32(id)]; ok {
			return int32(len(pg.Replicas)), nil
		}
	}
	return 0, errors.New("pg not exist")
}

func (m *PgMaps) GetPgExpectedId(pgId string) (int32, error) {
	array := strings.Split(pgId, ".")
	pool, _ := strconv.Atoi(array[0])
	id, _ := strconv.Atoi(array[1])
	if pgmap, ok := m.Pgmaps[int32(pool)]; ok {
		if pg, ok := pgmap.Pgmap[int32(id)]; ok {
			for _, rep := range pg.Replicas {
				if rep.OsdId == pg.ExpectedPrimaryId {
					return rep.ReplicaIndex, nil
				}
			}
		}
	}
	return 0, errors.New("pg not exist")
}

func (m *PgMaps) GetPgReplicas(pgId string) ([]PgReplica, error) {
	array := strings.Split(pgId, ".")
	pool, _ := strconv.Atoi(array[0])
	id, _ := strconv.Atoi(array[1])
	if pgmap, ok := m.Pgmaps[int32(pool)]; ok {
		if pg, ok := pgmap.Pgmap[int32(id)]; ok {
			return pg.Replicas, nil
		}
	}
	return nil, errors.New("pg not exist")
}
