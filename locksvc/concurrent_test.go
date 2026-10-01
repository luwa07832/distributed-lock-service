package locksvc

import (
	"sync"
	"testing"
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
