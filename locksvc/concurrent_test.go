package locksvc

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestConcurrentAcquireProducesSingleHolderAndFifoQueue(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.Acquire("res", "seed", 10); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := string(rune('a' + i))
			if _, err := svc.Acquire("res", owner, 10); err != nil && err != ErrDuplicateWaiter {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent acquire: %v", err)
	}

	state, _ := svc.GetResource("res")
	if state.OwnerID != "seed" {
		t.Fatalf("holder = %q, want seed", state.OwnerID)
	}
	if len(state.Waiting) != n {
		t.Fatalf("waiting len = %d, want %d", len(state.Waiting), n)
	}
	seen := make(map[string]bool, n)
	for _, waiter := range state.Waiting {
		if seen[waiter.OwnerID] {
			t.Fatalf("duplicate waiter %q", waiter.OwnerID)
		}
		seen[waiter.OwnerID] = true
	}
}

func TestRepeatedReleaseEventuallyFreesResource(t *testing.T) {
	svc, _ := newTestService(t)

	if _, err := svc.Acquire("res", "a", 10); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	for i := 0; i < 20; i++ {
		svc.Acquire("res", "a", 10) // reentries
	}
	for i := 0; i < 20; i++ {
		outcome, err := svc.Release("res", "a")
		if err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
		if outcome.Status != StatusReentryDecrement {
			t.Fatalf("release %d status = %q", i, outcome.Status)
		}
	}
	final, err := svc.Release("res", "a")
	if err != nil || final.Status != StatusReleased {
		t.Fatalf("final release = %+v, %v", final, err)
	}
	if final.State.Exists {
		t.Fatalf("resource still exists: %+v", final.State)
	}
}

func TestConcurrentCancelVersusExpiryTransferIsDeterministic(t *testing.T) {
	for i := 0; i < 200; i++ {
		svc, clock := newTestService(t)

		if _, err := svc.Acquire("res", "a", 30); err != nil {
			t.Fatalf("acquire a: %v", err)
		}
		if _, err := svc.Acquire("res", "b", 45); err != nil {
			t.Fatalf("acquire b: %v", err)
		}
		clock.add(31 * time.Second)

		canceled := make(chan error, 1)
		swept := make(chan struct{}, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := svc.CancelWaiting("res", "b")
			canceled <- err
		}()
		go func() {
			defer wg.Done()
			_, _ = svc.GetResource("res")
			swept <- struct{}{}
		}()
		wg.Wait()
		<-swept
		err := <-canceled

		view, _ := svc.GetResource("res")
		switch {
		case err == nil:
			// Cancel won: the expired lease freed the resource without promoting b.
			if view.Locked || view.OwnerID != "" {
				t.Fatalf("iter %d: canceled waiter was promoted: %+v", i, view)
			}
			if len(view.Waiting) != 0 {
				t.Fatalf("iter %d: waiting = %+v", i, view.Waiting)
			}
		case errors.Is(err, ErrNotWaiting):
			// Transfer won: b holds the new lock and must not have lost it.
			if view.OwnerID != "b" || !view.Locked {
				t.Fatalf("iter %d: promoted holder lost the lock: %+v", i, view)
			}
		default:
			t.Fatalf("iter %d: unexpected cancel error: %v", i, err)
		}
	}
}
