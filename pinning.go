package siastorage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
)

const (
	// pinBatchSize is the maximum number of slabs sent to the indexer in a
	// single PinSlabs request.
	pinBatchSize = 50

	// maxPinAttempts is the number of times a slab pin is attempted before the
	// upload fails.
	maxPinAttempts = 3

	// pinRetryDelay is the delay before the first pin retry. It doubles on
	// every subsequent attempt.
	pinRetryDelay = 250 * time.Millisecond
)

// pinSlab pins a single slab, retrying transient errors with exponential
// backoff. It returns the error of the last attempt once maxPinAttempts is
// reached, failing the upload.
func (s *SDK) pinSlab(ctx context.Context, slab slabs.SlabSlice) error {
	params := slab.Pin()
	if err := params.Validate(); err != nil {
		return fmt.Errorf("slab invalid: %w", err)
	}

	delay := pinRetryDelay
	for attempt := 1; ; attempt++ {
		err := s.pinSlabs(ctx, params)
		if err == nil {
			return nil
		} else if attempt == maxPinAttempts || ctx.Err() != nil {
			return err
		}

		s.log.Debug("failed to pin slab, retrying", zap.Int("attempt", attempt), zap.Duration("delay", delay), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
}

// pinSlabs validates the given slabs and pins them in batches of pinBatchSize,
// checking the ids returned by the indexer. Errors are not retried.
func (s *SDK) pinSlabs(ctx context.Context, params ...slabs.SlabPinParams) error {
	for i := range params {
		if err := params[i].Validate(); err != nil {
			return fmt.Errorf("slab %d invalid: %w", i, err)
		}
	}

	for i := 0; i < len(params); i += pinBatchSize {
		batch := params[i:min(i+pinBatchSize, len(params))]

		ids, err := s.app.PinSlabs(ctx, s.appKey, batch...)
		if err != nil {
			return fmt.Errorf("failed to pin slabs: %w", err)
		} else if err := validateSlabIDs(batch, ids); err != nil {
			return fmt.Errorf("batch at slab %d: %w", i, err)
		}
	}
	return nil
}

func validateSlabIDs(params []slabs.SlabPinParams, ids []slabs.SlabID) error {
	if len(ids) != len(params) {
		return fmt.Errorf("pinned %d slabs, expected %d", len(ids), len(params))
	}
	for i, id := range ids {
		if expected := params[i].Digest(); id != expected {
			return fmt.Errorf("slab %d: pinned id %s does not match expected id %s", i, id, expected)
		}
	}
	return nil
}

// isUnpinnedSlabErr reports whether the indexer rejected an object for
// referencing an unpinned slab. The app client flattens the response into an
// HTTP error, so the message is all there is to match on.
func isUnpinnedSlabErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), slabs.ErrObjectUnpinnedSlab.Error())
}
