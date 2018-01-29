package protos

const (
	PG_STATE_CREATING = (1 << 0) // creating
	PG_STATE_ACTIVE   = (1 << 1) // i am active.  (primary: replicas too)
	PG_STATE_CLEAN    = (1 << 2) // peers are complete, clean of stray replicas.
	PG_STATE_DOWN     = (1 << 4) // a needed replica is down, PG offline
	PG_STATE_REPLAY   = (1 << 5) // crashed, waiting for replay
	//e PG_STATE_STRAY      (1<<6)  // i must notify the primary i exist.
	PG_STATE_MIGRATING        = (1 << 7)  // i am splitting
	PG_STATE_SCRUBBING        = (1 << 8)  // scrubbing
	PG_STATE_SCRUBQ           = (1 << 9)  // queued for scrub
	PG_STATE_DEGRADED         = (1 << 10) // pg contains objects with reduced redundancy
	PG_STATE_INCONSISTENT     = (1 << 11) // pg replicas are inconsistent (but shouldn't be)
	PG_STATE_PEERING          = (1 << 12) // pg is (re)peering
	PG_STATE_REPAIR           = (1 << 13) // pg should repair on next scrub
	PG_STATE_RECOVERING       = (1 << 14) // pg is recovering/migrating objects
	PG_STATE_BACKFILL_WAIT    = (1 << 15) // [active] reserving backfill
	PG_STATE_INCOMPLETE       = (1 << 16) // incomplete content, peering failed.
	PG_STATE_STALE            = (1 << 17) // our state for this pg is stale, unknown.
	PG_STATE_REMAPPED         = (1 << 18) // pg is explicitly remapped to different OSDs than CRUSH
	PG_STATE_DEEP_SCRUB       = (1 << 19) // deep scrub: check CRC32 on files
	PG_STATE_BACKFILL         = (1 << 20) // [active] backfilling pg content
	PG_STATE_BACKFILL_TOOFULL = (1 << 21) // backfill can't proceed: too full
	PG_STATE_RECOVERY_WAIT    = (1 << 22) // waiting for recovery reservations
	PG_STATE_UNDERSIZED       = (1 << 23) // pg acting < pool size
	PG_STATE_ACTIVATING       = (1 << 24) // pg is peered but not yet active
	PG_STATE_PEERED           = (1 << 25) // peered, cannot go active, can recover
	PG_STATE_UNINITIAL        = (1 << 26) // unstable stable, maybe change at next term
)

func Pg_state_string(state int32) string {
	combine_string := ""
	if state&PG_STATE_STALE > 0 {
		combine_string += "stale+"
	}
	if state&PG_STATE_CREATING > 0 {
		combine_string += "creating+"
	}
	if state&PG_STATE_ACTIVE > 0 {
		combine_string += "active+"
	}
	if state&PG_STATE_ACTIVATING > 0 {
		combine_string += "activating+"
	}
	if state&PG_STATE_CLEAN > 0 {
		combine_string += "clean+"
	}
	if state&PG_STATE_RECOVERY_WAIT > 0 {
		combine_string += "recovery_wait+"
	}
	if state&PG_STATE_RECOVERING > 0 {
		combine_string += "recovering+"
	}
	if state&PG_STATE_DOWN > 0 {
		combine_string += "down+"
	}
	if state&PG_STATE_REPLAY > 0 {
		combine_string += "replay+"
	}
	if state&PG_STATE_MIGRATING > 0 {
		combine_string += "migrating+"
	}
	if state&PG_STATE_UNDERSIZED > 0 {
		combine_string += "undersized+"
	}
	if state&PG_STATE_DEGRADED > 0 {
		combine_string += "degraded+"
	}
	if state&PG_STATE_REMAPPED > 0 {
		combine_string += "remapped+"
	}
	if state&PG_STATE_SCRUBBING > 0 {
		combine_string += "scrubbing+"
	}
	if state&PG_STATE_DEEP_SCRUB > 0 {
		combine_string += "deep+"
	}
	if state&PG_STATE_SCRUBQ > 0 {
		combine_string += "scrubq+"
	}
	if state&PG_STATE_INCONSISTENT > 0 {
		combine_string += "inconsistent+"
	}
	if state&PG_STATE_PEERING > 0 {
		combine_string += "repair+"
	}
	if state&PG_STATE_REPAIR > 0 {
		combine_string += "peering+"
	}
	if state&PG_STATE_BACKFILL_WAIT > 0 && !(state&PG_STATE_BACKFILL > 0) {
		combine_string += "wait_backfill+"
	}
	if state&PG_STATE_BACKFILL > 0 {
		combine_string += "backfilling+"
	}
	if state&PG_STATE_BACKFILL_TOOFULL > 0 {
		combine_string += "backfill_toofull+"
	}
	if state&PG_STATE_INCOMPLETE > 0 {
		combine_string += "incomplete+"
	}
	if state&PG_STATE_PEERED > 0 {
		combine_string += "peered+"
	}
	if state&PG_STATE_UNINITIAL > 0 {
		combine_string += "uninitial+"
	}

	if len(combine_string) > 0 {
		return combine_string
	} else {
		return "inactive"
	}

}

func (ps *PgStatus) StatusSet(m int32) {
	ps.Status = ps.Status | m
}

func (ps *PgStatus) StatusTest(m int32) bool {
	return ps.Status&m != 0
}

func (ps *PgStatus) StatusClear(m int32) {
	ps.Status &= ^m
}
