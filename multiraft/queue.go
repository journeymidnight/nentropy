package multiraft

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/journeymidnight/nentropy/helper"
	"github.com/journeymidnight/nentropy/multiraft/multiraftbase"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/journeymidnight/nentropy/util/stop"
	"github.com/journeymidnight/nentropy/util/syncutil"
	"github.com/journeymidnight/nentropy/util/timeutil"
)

const (
	// purgatoryReportInterval is the duration between reports on
	// purgatory status.
	purgatoryReportInterval = 10 * time.Minute
	// defaultProcessTimeout is the timeout when processing a replica.
	// The timeout prevents a queue from getting stuck on a replica.
	// For example, a replica whose range is not reachable for quorum.
	defaultProcessTimeout = 1 * time.Minute
	// defaultQueueMaxSize is the default max size for a queue.
	defaultQueueMaxSize = 10000
)

// a purgatoryError indicates a replica processing failure which indicates
// the replica can be placed into purgatory for faster retries when the
// failure condition changes.
type purgatoryError interface {
	error
	purgatoryErrorMarker() // dummy method for unique interface
}

// A replicaItem holds a replica and its priority for use with a priority queue.
type replicaItem struct {
	value    multiraftbase.GroupID
	priority float64
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A priorityQueue implements heap.Interface and holds replicaItems.
type priorityQueue []*replicaItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index, pq[j].index = i, j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*replicaItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	old[n-1] = nil  // for gc
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority of a replicaItem in the queue.
func (pq *priorityQueue) update(item *replicaItem, priority float64) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

var (
	errQueueDisabled     = errors.New("queue disabled")
	errQueueStopped      = errors.New("queue stopped")
	errReplicaNotAddable = errors.New("replica shouldn't be added to queue")
)

func isExpectedQueueError(err error) bool {
	cause := errors.Cause(err)
	return err == nil || cause == errQueueDisabled || cause == errReplicaNotAddable
}

// shouldQueueAgain is a helper function to determine whether the
// replica should be queued according to the current time, the last
// time the replica was processed, and the minimum interval between
// successive processing. Specifying minInterval=0 queues all replicas.
// Returns a bool for whether to queue as well as a priority based
// on how long it's been since last processed.
func shouldQueueAgain(minInterval time.Duration) (bool, float64) {
	return true, 0
}

type queueImpl interface {
	// shouldQueue accepts current time, a replica, and the system config
	// and returns whether it should be queued and if so, at what priority.
	// The Replica is guaranteed to be initialized.
	shouldQueue(
		context.Context, *Replica, multiraftbase.SystemConfig,
	) (shouldQueue bool, priority float64)

	// process accepts lease status, a replica, and the system config
	// and executes queue-specific work on it. The Replica is guaranteed
	// to be initialized.
	process(context.Context, *Replica, multiraftbase.SystemConfig) error

	// timer returns a duration to wait between processing the next item
	// from the queue. The duration of the last processing of a replica
	// is supplied as an argument.
	timer(time.Duration) time.Duration

	// purgatoryChan returns a channel that is signaled when it's time
	// to retry replicas which have been relegated to purgatory due to
	// failures. If purgatoryChan returns nil, failing replicas are not
	// sent to purgatory.
	purgatoryChan() <-chan struct{}
}

type queueConfig struct {
	// maxSize is the maximum number of replicas to queue.
	maxSize int
	// needsLease controls whether this queue requires the range lease to
	// operate on a replica.
	needsLease bool
	// needsSystemConfig controls whether this queue requires a valid copy of the
	// system config to operate on a replica. Not all queues require it, and it's
	// unsafe for certain queues to wait on it. For example, a raft snapshot may
	// be needed in order to make it possible for the system config to become
	// available (as observed in #16268), so the raft snapshot queue can't
	// require the system config to already be available.
	needsSystemConfig bool
	// acceptsUnsplitRanges controls whether this queue can process ranges that
	// need to be split due to zone config settings. Ranges are checked before
	// calling queueImpl.shouldQueue and queueImpl.process.
	// This is to avoid giving the queue a replica that spans multiple config
	// zones (which might make the action of the queue ambiguous - e.g. we don't
	// want to try to replicate a range until we know which zone it is in and
	// therefore how many replicas are required).
	acceptsUnsplitRanges bool
	// processTimeout is the timeout for processing a replica.
	processTimeout time.Duration
}

// baseQueue is the base implementation of the replicaQueue interface.
// Queue implementations should embed a baseQueue and implement queueImpl.
//
// In addition to normal processing of replicas via the replica
// scanner, queues have an optional notion of purgatory, where
// replicas which fail queue processing with a retryable error may be
// sent such that they will be quickly retried when the failure
// condition changes. Queue implementations opt in for purgatory by
// implementing the purgatoryChan method of queueImpl such that it
// returns a non-nil channel.
type baseQueue struct {
	helper.AmbientContext

	name string
	// The constructor of the queueImpl structure MUST return a pointer.
	// This is because assigning queueImpl to a function-local, then
	// passing a pointer to it to `makeBaseQueue`, and then returning it
	// from the constructor function will return a queueImpl containing
	// a pointer to a structure which is a copy of the one within which
	// it is contained. DANGER.
	impl  queueImpl
	store *Store
	queueConfig
	incoming chan struct{} // Channel signaled when a new replica is added to the queue.
	mu       struct {
		sync.Locker                                        // Protects all variables in the mu struct
		priorityQ   priorityQueue                          // The priority queue
		replicas    map[multiraftbase.GroupID]*replicaItem // Map from GroupID to replicaItem (for updating priority)
		purgatory   map[multiraftbase.GroupID]error        // Map of replicas to processing errors
		stopped     bool
		// Some tests in this package disable queues.
		disabled bool
	}

	// processMu synchronizes execution of processing for a single queue,
	// ensuring that we never process more than a single replica at a time. This
	// is needed because both the main processing loop and the purgatory loop can
	// process replicas.
	processMu sync.Locker
}

// newBaseQueue returns a new instance of baseQueue with the specified
// shouldQueue function to determine which replicas to queue and maxSize to
// limit the growth of the queue. Note that maxSize doesn't prevent new
// replicas from being added, it just limits the total size. Higher priority
// replicas can still be added; their addition simply removes the lowest
// priority replica.
func newBaseQueue(
	name string, impl queueImpl, store *Store, cfg queueConfig,
) *baseQueue {
	// Use the default process timeout if none specified.
	if cfg.processTimeout == 0 {
		cfg.processTimeout = defaultProcessTimeout
	}

	ambient := store.cfg.AmbientCtx

	bq := baseQueue{
		AmbientContext: ambient,
		name:           name,
		impl:           impl,
		store:          store,
		queueConfig:    cfg,
		incoming:       make(chan struct{}, 1),
	}
	bq.mu.Locker = new(syncutil.Mutex)
	bq.mu.replicas = map[multiraftbase.GroupID]*replicaItem{}
	bq.processMu = new(syncutil.Mutex)

	return &bq
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.mu.priorityQ.Len()
}

// PurgatoryLength returns the current size of purgatory.
func (bq *baseQueue) PurgatoryLength() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return len(bq.mu.purgatory)
}

// SetDisabled turns queue processing off or on as directed.
func (bq *baseQueue) SetDisabled(disabled bool) {
	bq.mu.Lock()
	bq.mu.disabled = disabled
	bq.mu.Unlock()
}

// Disabled returns true is the queue is currently disabled.
func (bq *baseQueue) Disabled() bool {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.mu.disabled
}

// Start launches a goroutine to process entries in the queue. The
// provided stopper is used to finish processing.
func (bq *baseQueue) Start(stopper *stop.Stopper) {
	bq.processLoop(stopper)
}

// Add adds the specified replica to the queue, regardless of the
// return value of bq.shouldQueue. The replica is added with specified
// priority. If the queue is too full, the replica may not be added,
// as the replica with the lowest priority will be dropped. Returns
// (true, nil) if the replica was added, (false, nil) if the replica
// was already present, and (false, err) if the replica could not be
// added for any other reason.
func (bq *baseQueue) Add(repl *Replica, priority float64) (bool, error) {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	ctx := repl.AnnotateCtx(bq.AnnotateCtx(context.TODO()))
	return bq.addInternal(ctx, repl.Desc(), true, priority)
}

// MaybeAdd adds the specified replica if bq.shouldQueue specifies it
// should be queued. Replicas are added to the queue using the priority
// returned by bq.shouldQueue. If the queue is too full, the replica may
// not be added, as the replica with the lowest priority will be
// dropped.
func (bq *baseQueue) MaybeAdd(repl *Replica) {
	ctx := repl.AnnotateCtx(bq.AnnotateCtx(context.TODO()))

	// Load the system config if it's needed.
	var cfg multiraftbase.SystemConfig

	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.mu.stopped || bq.mu.disabled {
		return
	}

	if !repl.IsInitialized() {
		return
	}

	should, priority := bq.impl.shouldQueue(ctx, repl, cfg)
	if _, err := bq.addInternal(ctx, repl.Desc(), should, priority); !isExpectedQueueError(err) {
		helper.Logger.Printf(5, "unable to add: %s", err)
	}
}

// addInternal adds the replica the queue with specified priority. If
// the replica is already queued at a lower priority, updates the existing
// priority. Expects the queue lock to be held by caller.
func (bq *baseQueue) addInternal(
	ctx context.Context, desc *multiraftbase.GroupDescriptor, should bool, priority float64,
) (bool, error) {
	if bq.mu.stopped {
		return false, errQueueStopped
	}

	if bq.mu.disabled {
		helper.Logger.Printf(5, "queue disabled")
		return false, errQueueDisabled
	}

	if !desc.IsInitialized() {
		// We checked this above in MaybeAdd(), but we need to check it
		// again for Add().
		return false, errors.New("replica not initialized")
	}

	// If the replica is currently in purgatory, don't re-add it.
	if _, ok := bq.mu.purgatory[desc.GroupID]; ok {
		return false, nil
	}

	// Note that even though the caller said not to queue the replica, we don't
	// want to remove it if it's already been queued. It may have been added by
	// a queuer that knows more than this one.
	if !should {
		return false, errReplicaNotAddable
	}

	item, ok := bq.mu.replicas[desc.GroupID]
	if ok {
		// Replica has already been added but at a lower priority; update priority.
		// Don't lower it since the previous queuer may have known more than this
		// one does.
		if priority > item.priority {
			bq.mu.priorityQ.update(item, priority)
		}
		return false, nil
	}

	item = &replicaItem{value: desc.GroupID, priority: priority}
	bq.add(item)

	// If adding this replica has pushed the queue past its maximum size,
	// remove the lowest priority element.
	if pqLen := bq.mu.priorityQ.Len(); pqLen > bq.maxSize {
		bq.remove(bq.mu.priorityQ[pqLen-1])
	}
	// Signal the processLoop that a replica has been added.
	select {
	case bq.incoming <- struct{}{}:
	default:
		// No need to signal again.
	}
	return true, nil
}

// MaybeRemove removes the specified replica from the queue if enqueued.
func (bq *baseQueue) MaybeRemove(groupID multiraftbase.GroupID) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.mu.stopped {
		return
	}

	if item, ok := bq.mu.replicas[groupID]; ok {
		//_ := bq.AnnotateCtx(context.TODO())
		bq.remove(item)
	}
}

// processLoop processes the entries in the queue until the provided
// stopper signals exit.
func (bq *baseQueue) processLoop(stopper *stop.Stopper) {
	ctx := bq.AnnotateCtx(context.Background())
	stopper.RunWorker(ctx, func(ctx context.Context) {
		defer func() {
			bq.mu.Lock()
			bq.mu.stopped = true
			bq.mu.Unlock()
		}()

		// nextTime is initially nil; we don't start any timers until the queue
		// becomes non-empty.
		var nextTime <-chan time.Time

		immediately := make(chan time.Time)
		close(immediately)

		for {
			select {
			// Exit on stopper.
			case <-stopper.ShouldStop():
				return

				// Incoming signal sets the next time to process if there were previously
				// no replicas in the queue.
			case <-bq.incoming:
				if nextTime == nil {
					// When a replica is added, wake up immediately. This is mainly
					// to facilitate testing without unnecessary sleeps.
					nextTime = immediately

					// In case we're in a test, still block on the impl.
					bq.impl.timer(0)
				}
				// Process replicas as the timer expires.
			case <-nextTime:
				repl := bq.pop()
				var duration time.Duration
				if repl != nil {
					annotatedCtx := repl.AnnotateCtx(ctx)
					if stopper.RunTask(
						annotatedCtx, fmt.Sprintf("storage.%s: processing replica", bq.name),
						func(annotatedCtx context.Context) {
							start := timeutil.Now()
							if err := bq.processReplica(annotatedCtx, repl); err != nil {
								// Maybe add failing replica to purgatory if the queue supports it.
								bq.maybeAddToPurgatory(annotatedCtx, repl, err, stopper)
							}
							duration = timeutil.Since(start)
						}) != nil {
						return
					}
				}
				if bq.Length() == 0 {
					nextTime = nil
				} else {
					nextTime = time.After(bq.impl.timer(duration))
				}
			}
		}
	})
}

// processReplica processes a single replica. This should not be
// called externally to the queue. bq.mu.Lock must not be held
// while calling this method.
func (bq *baseQueue) processReplica(
	queueCtx context.Context, repl *Replica) error {

	bq.processMu.Lock()
	defer bq.processMu.Unlock()

	// Load the system config if it's needed.
	var cfg multiraftbase.SystemConfig

	// Also add the Replica annotations to ctx.
	ctx := repl.AnnotateCtx(queueCtx)
	ctx, cancel := context.WithTimeout(ctx, bq.processTimeout)
	defer cancel()

	if !repl.IsInitialized() {
		// We checked this when adding the replica, but we need to check it again
		// in case this is a different replica with the same range ID (see #14193).
		return errors.New("cannot process uninitialized replica")
	}

	if err := repl.IsDestroyed(); err != nil {
		return nil
	}

	if err := bq.impl.process(ctx, repl, cfg); err != nil {
		return err
	}

	return nil
}

// maybeAddToPurgatory possibly adds the specified replica to the
// purgatory queue, which holds replicas which have failed
// processing. To be added, the failing error must implement
// purgatoryError and the queue implementation must have its own
// mechanism for signaling re-processing of replicas held in
// purgatory.
func (bq *baseQueue) maybeAddToPurgatory(
	ctx context.Context, repl *Replica, triggeringErr error, stopper *stop.Stopper,
) {
	// Check whether the failure is a purgatory error and whether the queue supports it.
	if _, ok := errors.Cause(triggeringErr).(purgatoryError); !ok || bq.impl.purgatoryChan() == nil {
		helper.Logger.Printf(5, "%s", triggeringErr)
		return
	}
	bq.mu.Lock()
	defer bq.mu.Unlock()

	// First, check whether the replica has already been re-added to queue.
	if _, ok := bq.mu.replicas[repl.GroupID]; ok {
		return
	}

	item := &replicaItem{value: repl.GroupID}
	bq.mu.replicas[repl.GroupID] = item

	// If purgatory already exists, just add to the map and we're done.
	if bq.mu.purgatory != nil {
		bq.mu.purgatory[repl.GroupID] = triggeringErr
		return
	}

	// Otherwise, create purgatory and start processing.
	bq.mu.purgatory = map[multiraftbase.GroupID]error{
		repl.GroupID: triggeringErr,
	}

	workerCtx := bq.AnnotateCtx(context.Background())
	stopper.RunWorker(workerCtx, func(ctx context.Context) {
		ticker := time.NewTicker(purgatoryReportInterval)
		for {
			select {
			case <-bq.impl.purgatoryChan():
				// Remove all items from purgatory into a copied slice.
				bq.mu.Lock()
				ranges := make([]multiraftbase.GroupID, 0, len(bq.mu.purgatory))
				for groupID := range bq.mu.purgatory {
					item := bq.mu.replicas[groupID]
					ranges = append(ranges, item.value)
					bq.remove(item)
				}
				bq.mu.Unlock()
				for _, id := range ranges {
					repl, err := bq.store.GetReplica(id)
					if err != nil {
						helper.Logger.Printf(5, "range %s no longer exists on store: %s", id, err)
						return
					}
					annotatedCtx := repl.AnnotateCtx(ctx)
					if stopper.RunTask(
						annotatedCtx, fmt.Sprintf("storage.%s: purgatory processing replica", bq.name),
						func(annotatedCtx context.Context) {
							if err := bq.processReplica(annotatedCtx, repl); err != nil {
								bq.maybeAddToPurgatory(annotatedCtx, repl, err, stopper)
							}
						}) != nil {
						return
					}
				}
				bq.mu.Lock()
				if len(bq.mu.purgatory) == 0 {
					helper.Logger.Printf(5, "purgatory is now empty")
					bq.mu.purgatory = nil
					bq.mu.Unlock()
					return
				}
				bq.mu.Unlock()
			case <-ticker.C:
				// Report purgatory status.
				bq.mu.Lock()
				errMap := map[string]int{}
				for _, err := range bq.mu.purgatory {
					errMap[err.Error()]++
				}
				bq.mu.Unlock()
				for errStr, count := range errMap {
					helper.Logger.Printf(5, "%d replicas failing with %q", count, errStr)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// pop dequeues the highest priority replica, if any, in the queue. Expects
// mutex to be locked.
func (bq *baseQueue) pop() *Replica {
	var repl *Replica
	for repl == nil {
		bq.mu.Lock()

		if bq.mu.priorityQ.Len() == 0 {
			bq.mu.Unlock()
			return nil
		}
		item := heap.Pop(&bq.mu.priorityQ).(*replicaItem)
		delete(bq.mu.replicas, item.value)
		bq.mu.Unlock()
		repl, _ = bq.store.GetReplica(item.value)
	}
	return repl
}

// add adds an element to the priority queue. Caller must hold mutex.
func (bq *baseQueue) add(item *replicaItem) {
	heap.Push(&bq.mu.priorityQ, item)
	bq.mu.replicas[item.value] = item
}

// remove removes an element from purgatory (if it's experienced an
// error) or from the priority queue by index. Caller must hold mutex.
func (bq *baseQueue) remove(item *replicaItem) {
	if _, ok := bq.mu.purgatory[item.value]; ok {
		delete(bq.mu.purgatory, item.value)
	} else {
		heap.Remove(&bq.mu.priorityQ, item.index)
	}
	delete(bq.mu.replicas, item.value)
}

// IsDestroyed returns a non-nil error if the replica has been destroyed.
func (r *Replica) IsDestroyed() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.destroyed
}
