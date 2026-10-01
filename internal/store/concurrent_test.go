package store

import (
	"errors"
	"sync"
	"testing"
)

func TestConcurrentAcquiresNeverDuplicateOrLose(t *testing.T) {
	st := newTestStore(t)

	const owners = 40
	var wg sync.WaitGroup
	for i := 0; i < owners; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := "owner-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
			_, err := st.Acquire("hot", owner, 30)
			if err != nil && !errors.Is(err, ErrDuplicateWaiting) {
				t.Errorf("acquire %s: %v", owner, err)
			}
		}(i)
	}
	wg.Wait()

	state, err := st.GetState("hot")
	if err != nil {
		t.Fatalf("state: %v", err)
	}

	seen := map[string]bool{state.OwnerID: true}
	for _, waiting := range state.WaitingOwners {
		if seen[waiting.OwnerID] {
			t.Fatalf("owner %q appears twice (holder=%s queue=%v)",
				waiting.OwnerID, state.OwnerID, state.WaitingOwners)
		}
		seen[waiting.OwnerID] = true
	}
	if len(seen) != owners {
		t.Fatalf("participants = %d, want %d; queue=%v", len(seen), owners, state.WaitingOwners)
	}
	if len(state.WaitingOwners) != owners-1 {
		t.Fatalf("waiting len = %d, want %d", len(state.WaitingOwners), owners-1)
	}
}

func TestConcurrentReleasesKeepSingleHolder(t *testing.T) {
	st := newTestStore(t)

	st.Acquire("res", "holder", 10)
	for i := 0; i < 20; i++ {
		w := "w" + string(rune('a'+i))
		st.Acquire("res", w, 10)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				st.Release("res", "holder")
				return
			}
			st.Reenter("res", "holder")
		}(i)
	}
	wg.Wait()

	state, err := st.GetState("res")
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.OwnerID == "" {
		t.Fatalf("no holder after mixed reentries/releases")
	}
	// Holder plus its queued others must stay a partition: no owner in both places.
	queue := map[string]bool{}
	for _, waiting := range state.WaitingOwners {
		if waiting.OwnerID == state.OwnerID {
			t.Fatalf("holder %q also queued", state.OwnerID)
		}
		queue[waiting.OwnerID] = true
	}
	if len(queue) != len(state.WaitingOwners) {
		t.Fatalf("duplicate waiter entries")
	}
}
