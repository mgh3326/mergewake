package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"regexp"
	"time"

	"github.com/mgh3326/mergewake/internal/server/state"
)

type Key struct {
	Repository string
	Number     uint64
}
type Fence struct {
	Owner string
	Epoch uint64
}
type Class string

const (
	Automatic Class = "automatic"
	Operator  Class = "operator"
)

type Input struct {
	Key             Key
	Source, ID      string
	ExpectedVersion uint64
	Event           state.Event
	Class           Class
}
type Applied struct {
	Snapshot  state.Snapshot
	Duplicate bool
}
type Claim struct {
	ID, Epoch, Generation uint64
	Key                   Key
	Kind                  state.EffectKind
	Class                 Class
	RequestID             string
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var repository = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$`)
var commitID = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)

// Acquire renews the active owner's lease or takes an expired lease with a new
// epoch. Owners are unique process identities, not reusable host names.
func (s *Store) Acquire(ctx context.Context, owner string, ttl time.Duration) (Fence, error) {
	if !identifier.MatchString(owner) || ttl < time.Second || ttl > 5*time.Minute {
		return Fence{}, ErrConfig
	}
	return transaction(ctx, s, func(tx *sql.Tx) (Fence, error) {
		_, err := tx.ExecContext(ctx, `INSERT IGNORE INTO mw_coordinator VALUES (1,'',0,'1970-01-01')`)
		if err != nil {
			return Fence{}, err
		}
		var current string
		var epoch uint64
		var live bool
		if err = tx.QueryRowContext(ctx, `SELECT owner,epoch,lease_until>UTC_TIMESTAMP(6) FROM mw_coordinator WHERE id=1 FOR UPDATE`).Scan(&current, &epoch, &live); err != nil {
			return Fence{}, err
		}
		if live && current != owner {
			return Fence{}, ErrFenced
		}
		if !live {
			epoch++
		}
		_, err = tx.ExecContext(ctx, `UPDATE mw_coordinator SET owner=?,epoch=?,lease_until=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)) WHERE id=1`, owner, epoch, ttl.Microseconds())
		return Fence{owner, epoch}, err
	})
}

func checkFence(ctx context.Context, tx *sql.Tx, f Fence) error {
	var owner string
	var epoch uint64
	var live bool
	if err := tx.QueryRowContext(ctx, `SELECT owner,epoch,lease_until>UTC_TIMESTAMP(6) FROM mw_coordinator WHERE id=1 FOR UPDATE`).Scan(&owner, &epoch, &live); err != nil {
		return err
	}
	if !live || owner != f.Owner || epoch != f.Epoch {
		return ErrFenced
	}
	return nil
}

func readPR(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, k Key, lock bool) (state.Snapshot, uint64, error) {
	query := `SELECT snapshot,next_sequence,generation,version FROM mw_prs WHERE repository=? AND number=?`
	if lock {
		query += " FOR UPDATE"
	}
	var raw []byte
	var seq uint64
	var generation, version uint64
	var snapshot state.Snapshot
	if err := q.QueryRowContext(ctx, query, k.Repository, k.Number).Scan(&raw, &seq, &generation, &version); err != nil {
		return snapshot, 0, err
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return snapshot, 0, ErrSchema
	}
	if !snapshot.Valid() || snapshot.Generation != generation || snapshot.Version != version {
		return snapshot, 0, ErrSchema
	}
	return snapshot, seq, nil
}

func (s *Store) Snapshot(ctx context.Context, k Key) (state.Snapshot, error) {
	snapshot, _, err := readPR(ctx, s.db, k, false)
	if err != nil {
		return state.Snapshot{}, ErrUnavailable
	}
	return snapshot, nil
}

// Apply atomically deduplicates an input, updates the aggregate and persists
// all resulting work. Operator class must be granted by the trusted API adapter.
func (s *Store) Apply(ctx context.Context, f Fence, in Input) (Applied, error) {
	if !repository.MatchString(in.Key.Repository) || in.Key.Number == 0 || !identifier.MatchString(in.Source) || len(in.Source) > 32 || !identifier.MatchString(in.ID) || (in.Class != Automatic && in.Class != Operator) {
		return Applied{}, ErrConfig
	}
	if in.Event.Head != "" && !commitID.MatchString(in.Event.Head) {
		return Applied{}, ErrConfig
	}
	if in.Event.Base != "" && !commitID.MatchString(in.Event.Base) {
		return Applied{}, ErrConfig
	}
	b, _ := json.Marshal(in)
	sum := sha256.Sum256(b)
	digest := hex.EncodeToString(sum[:])
	return transaction(ctx, s, func(tx *sql.Tx) (Applied, error) {
		if err := checkFence(ctx, tx, f); err != nil {
			return Applied{}, err
		}
		var prior string
		err := tx.QueryRowContext(ctx, `SELECT digest FROM mw_deliveries WHERE source=? AND delivery_id=? FOR UPDATE`, in.Source, in.ID).Scan(&prior)
		if err == nil {
			if prior != digest {
				return Applied{}, ErrConflict
			}
			snap, _, err := readPR(ctx, tx, in.Key, true)
			return Applied{snap, true}, err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Applied{}, err
		}
		_, err = tx.ExecContext(ctx, `INSERT IGNORE INTO mw_prs VALUES (?,?,0,0,?,0)`, in.Key.Repository, in.Key.Number, `{}`)
		if err != nil {
			return Applied{}, err
		}
		snap, seq, err := readPR(ctx, tx, in.Key, true)
		if err != nil {
			return Applied{}, err
		}
		if snap.Version != in.ExpectedVersion {
			return Applied{}, ErrConflict
		}
		transition, err := state.Reduce(snap, in.Event)
		if err != nil {
			return Applied{}, err
		}
		if err = saveTransition(ctx, tx, in.Key, snap, seq, transition, in.Event.Kind, in.Class); err != nil {
			return Applied{}, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO mw_deliveries VALUES (?,?,?,?,?,UTC_TIMESTAMP(6))`, in.Source, in.ID, digest, in.Key.Repository, in.Key.Number)
		return Applied{Snapshot: transition.Next}, err
	})
}

func saveTransition(ctx context.Context, tx *sql.Tx, k Key, old state.Snapshot, seq uint64, t state.Transition, event state.EventKind, class Class) error {
	if t.Next == old && len(t.Effects) == 0 {
		return nil
	}
	if t.Next.Generation != old.Generation || t.Next.Stage == state.Merged || t.Next.Stage == state.Closed {
		// Running attempts stay in the ledger and capacity count until confirmed.
		if _, err := tx.ExecContext(ctx, `UPDATE mw_jobs SET status='superseded' WHERE repository=? AND number=? AND (kind<>'escalate' OR ?) AND status IN ('queued','running','uncertain','system_blocked')`, k.Repository, k.Number, event == state.Resume); err != nil {
			return err
		}
	}
	for _, effect := range t.Effects {
		seq++
		scope := sha256.Sum256([]byte(fmt.Sprintf("%d/%s", t.Next.Version, effect)))
		_, err := tx.ExecContext(ctx, `INSERT INTO mw_jobs (repository,number,sequence,generation,scope,kind,class,status,available_at,epoch,lease_until,failures) VALUES (?,?,?,?,?,?,?,'queued',UTC_TIMESTAMP(6),0,'1970-01-01',0)`, k.Repository, k.Number, seq, t.Next.Generation, hex.EncodeToString(scope[:]), effect, class)
		if err != nil {
			return err
		}
	}
	raw, err := json.Marshal(t.Next)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE mw_prs SET generation=?,version=?,snapshot=?,next_sequence=? WHERE repository=? AND number=?`, t.Next.Generation, t.Next.Version, string(raw), seq, k.Repository, k.Number); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO mw_audit (repository,number,version,event_kind,occurred_at) VALUES (?,?,?,?,UTC_TIMESTAMP(6))`, k.Repository, k.Number, t.Next.Version, event)
	return err
}

// Claim commits attempt identity before execution. Expired running leases are
// not reclaimed automatically: remote absence must be confirmed explicitly.
func (s *Store) Claim(ctx context.Context, f Fence, kind state.EffectKind, class Class) (Claim, error) {
	if class != Automatic && class != Operator {
		return Claim{}, ErrConfig
	}
	if kind != state.Refresh && kind != state.Review && kind != state.Gate && kind != state.NotifyAuthor && kind != state.Escalate && kind != "fix" {
		return Claim{}, ErrConfig
	}
	return transaction(ctx, s, func(tx *sql.Tx) (Claim, error) {
		if err := checkFence(ctx, tx, f); err != nil {
			return Claim{}, err
		}
		var reviews, fixes int
		if err := tx.QueryRowContext(ctx, `SELECT review_slots,fix_slots FROM mw_capacity WHERE id=1 FOR UPDATE`).Scan(&reviews, &fixes); err != nil {
			return Claim{}, err
		}
		if reviews < 2 || fixes < 1 {
			return Claim{}, ErrSchema
		}
		if kind == state.Review || kind == "fix" {
			var total, automatic int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(j.class='automatic'),0) FROM mw_attempts a JOIN mw_jobs j ON j.id=a.job_id WHERE j.kind=? AND a.status IN ('running','uncertain')`, kind).Scan(&total, &automatic); err != nil {
				return Claim{}, err
			}
			if kind == state.Review && (total >= reviews || (class == Automatic && automatic >= reviews-1)) {
				return Claim{}, ErrNoJob
			}
			if kind == "fix" && total >= fixes {
				return Claim{}, ErrNoJob
			}
		}
		var c Claim
		c.Kind, c.Class = kind, class
		err := tx.QueryRowContext(ctx, `SELECT j.id,j.repository,j.number,j.generation,j.epoch FROM mw_jobs j WHERE j.status='queued' AND j.kind=? AND j.class=? AND j.available_at<=UTC_TIMESTAMP(6) AND (j.kind='escalate' OR NOT EXISTS (SELECT 1 FROM mw_jobs earlier WHERE earlier.repository=j.repository AND earlier.number=j.number AND earlier.sequence<j.sequence AND earlier.status IN ('queued','running','uncertain','system_blocked'))) ORDER BY j.id LIMIT 1 FOR UPDATE SKIP LOCKED`, kind, class).Scan(&c.ID, &c.Key.Repository, &c.Key.Number, &c.Generation, &c.Epoch)
		if errors.Is(err, sql.ErrNoRows) {
			return Claim{}, ErrNoJob
		}
		if err != nil {
			return Claim{}, err
		}
		c.Epoch++
		c.RequestID = uuid()
		if kind == state.Review {
			snap, seq, err := readPR(ctx, tx, c.Key, true)
			if err != nil {
				return Claim{}, err
			}
			if snap.Stage != state.Reviewing {
				e := state.Event{Kind: state.StartReview, Head: snap.Head, Generation: snap.Generation}
				t, err := state.Reduce(snap, e)
				if err != nil {
					return Claim{}, err
				}
				if err = saveTransition(ctx, tx, c.Key, snap, seq, t, e.Kind, class); err != nil {
					return Claim{}, err
				}
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE mw_jobs SET status='running',epoch=?,lease_until=TIMESTAMPADD(SECOND,60,UTC_TIMESTAMP(6)) WHERE id=?`, c.Epoch, c.ID); err != nil {
			return Claim{}, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO mw_attempts VALUES (?,?,?,'running',UTC_TIMESTAMP(6))`, c.ID, c.Epoch, c.RequestID)
		return c, err
	})
}

func uuid() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func loadAttempt(ctx context.Context, tx *sql.Tx, c Claim) (string, error) {
	var request, status string
	var epoch uint64
	err := tx.QueryRowContext(ctx, `SELECT a.request_uuid,a.status,j.epoch FROM mw_attempts a JOIN mw_jobs j ON j.id=a.job_id WHERE a.job_id=? AND a.epoch=? FOR UPDATE`, c.ID, c.Epoch).Scan(&request, &status, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrFenced
	}
	if err != nil {
		return "", err
	}
	if request != c.RequestID || epoch != c.Epoch {
		return "", ErrFenced
	}
	return status, nil
}

// Complete releases a confirmed terminal attempt and commits its state change
// and follow-up intents together. Nil event means an effect with no transition.
func (s *Store) Complete(ctx context.Context, f Fence, c Claim, event *state.Event) error {
	_, err := transaction(ctx, s, func(tx *sql.Tx) (bool, error) {
		if err := checkFence(ctx, tx, f); err != nil {
			return false, err
		}
		status, err := loadAttempt(ctx, tx, c)
		if err != nil {
			return false, err
		}
		if status == "complete" {
			return true, nil
		}
		if status != "running" && status != "uncertain" {
			return false, ErrFenced
		}
		var k Key
		var generation uint64
		if err = tx.QueryRowContext(ctx, `SELECT repository,number,generation,class FROM mw_jobs WHERE id=?`, c.ID).Scan(&k.Repository, &k.Number, &generation, &c.Class); err != nil {
			return false, err
		}
		snap, seq, err := readPR(ctx, tx, k, true)
		if err != nil {
			return false, err
		}
		jobStatus := "complete"
		completionEvent := event
		if completionEvent == nil && snap.Failures > 0 && snap.Stage != state.SystemBlocked {
			completionEvent = &state.Event{Kind: state.Recovered, Head: snap.Head, Generation: snap.Generation}
		}
		if snap.Generation != generation || snap.Stage == state.Closed || snap.Stage == state.Merged {
			jobStatus = "superseded"
		} else if completionEvent != nil {
			if completionEvent.Head != snap.Head || completionEvent.Generation != generation {
				return false, ErrFenced
			}
			t, err := state.Reduce(snap, *completionEvent)
			if err != nil {
				return false, err
			}
			if t.Next.Stage != state.SystemBlocked && t.Next.Failures > 0 {
				t.Next.Failures = 0
				if t.Next.Version == snap.Version {
					t.Next.Version++
				}
			}
			if err = saveTransition(ctx, tx, k, snap, seq, t, completionEvent.Kind, c.Class); err != nil {
				return false, err
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE mw_jobs SET status=? WHERE id=?`, jobStatus, c.ID); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `UPDATE mw_attempts SET status='complete' WHERE job_id=? AND epoch=?`, c.ID, c.Epoch)
		return true, err
	})
	return err
}

// Uncertain preserves capacity. Lease expiry or hub cache expiry never proves
// absence and never releases this attempt for another spawn.
func (s *Store) Uncertain(ctx context.Context, f Fence, c Claim) error {
	_, err := transaction(ctx, s, func(tx *sql.Tx) (bool, error) {
		if err := checkFence(ctx, tx, f); err != nil {
			return false, err
		}
		status, err := loadAttempt(ctx, tx, c)
		if err != nil {
			return false, err
		}
		if status != "running" && status != "uncertain" {
			return false, ErrFenced
		}
		if _, err = tx.ExecContext(ctx, `UPDATE mw_attempts SET status='uncertain' WHERE job_id=? AND epoch=?`, c.ID, c.Epoch); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `UPDATE mw_jobs SET status='uncertain' WHERE id=? AND status='running'`, c.ID)
		return true, err
	})
	return err
}

// ConfirmAbsent is called only after authoritative absence/termination proof.
// The next claim receives a new epoch/UUID; old completions remain fenced.
func (s *Store) ConfirmAbsent(ctx context.Context, f Fence, c Claim) error {
	_, err := transaction(ctx, s, func(tx *sql.Tx) (bool, error) {
		if err := checkFence(ctx, tx, f); err != nil {
			return false, err
		}
		status, err := loadAttempt(ctx, tx, c)
		if err != nil {
			return false, err
		}
		if status == "absent" {
			return true, nil
		}
		if status != "running" && status != "uncertain" {
			return false, ErrFenced
		}
		var jobStatus string
		var failures int
		if err = tx.QueryRowContext(ctx, `SELECT status,failures FROM mw_jobs WHERE id=?`, c.ID).Scan(&jobStatus, &failures); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE mw_attempts SET status='absent' WHERE job_id=? AND epoch=?`, c.ID, c.Epoch); err != nil {
			return false, err
		}
		if jobStatus == "superseded" {
			return true, nil
		}
		failures++
		jobStatus = "queued"
		if failures >= 3 {
			jobStatus = "system_blocked"
		}
		delay := RetryDelay(failures)
		delay += time.Duration(mathrand.Int64N(int64(delay/5) + 1))
		if _, err = tx.ExecContext(ctx, `UPDATE mw_jobs SET status=?,failures=?,available_at=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)) WHERE id=?`, jobStatus, failures, delay.Microseconds(), c.ID); err != nil {
			return false, err
		}
		var k Key
		if err = tx.QueryRowContext(ctx, `SELECT repository,number FROM mw_jobs WHERE id=?`, c.ID).Scan(&k.Repository, &k.Number); err != nil {
			return false, err
		}
		snap, seq, err := readPR(ctx, tx, k, true)
		if err != nil {
			return false, err
		}
		e := state.Event{Kind: state.Failure, Head: snap.Head, Generation: snap.Generation}
		t, err := state.Reduce(snap, e)
		if err != nil {
			return false, err
		}
		return true, saveTransition(ctx, tx, k, snap, seq, t, e.Kind, c.Class)
	})
	return err
}

func RetryDelay(failures int) time.Duration {
	if failures < 1 {
		return 0
	}
	return min(15*time.Minute, 30*time.Second*time.Duration(1<<min(failures-1, 5)))
}

// Outstanding restores remote reconciliation handles after coordinator restart.
// Pagination includes superseded jobs whose remote attempts still hold capacity.
func (s *Store) Outstanding(ctx context.Context, f Fence, after uint64, limit int) ([]Claim, error) {
	if limit < 1 || limit > 100 {
		return nil, ErrConfig
	}
	return transaction(ctx, s, func(tx *sql.Tx) ([]Claim, error) {
		if err := checkFence(ctx, tx, f); err != nil {
			return nil, err
		}
		rows, err := tx.QueryContext(ctx, `SELECT j.id,a.epoch,j.repository,j.number,j.generation,j.kind,j.class,a.request_uuid FROM mw_attempts a JOIN mw_jobs j ON j.id=a.job_id WHERE a.status IN ('running','uncertain') AND j.id>? ORDER BY j.id LIMIT ?`, after, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var result []Claim
		for rows.Next() {
			var c Claim
			if err = rows.Scan(&c.ID, &c.Epoch, &c.Key.Repository, &c.Key.Number, &c.Generation, &c.Kind, &c.Class, &c.RequestID); err != nil {
				return nil, err
			}
			result = append(result, c)
		}
		return result, rows.Err()
	})
}
