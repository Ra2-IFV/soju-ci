package msgstore

import (
	"iter"
	"reflect"
	"testing"
	"time"
)

func assertSeq2[K, V comparable](t *testing.T, actual iter.Seq2[K, V], expected []struct {
	k K
	v V
}) {
	t.Helper()

	ok := true
	for actualK, actualV := range actual {
		if len(expected) == 0 {
			t.Errorf("Unexpected additional element: {k:%v v:%v}", actualK, actualV)
			ok = false
			continue
		}
		if !reflect.DeepEqual(actualK, expected[0].k) || !reflect.DeepEqual(actualV, expected[0].v) {
			t.Errorf("expected %+v got {k:%+v v:%+v}", expected[0], actualK, actualV)
			ok = false
		}
		expected = expected[1:]
	}

	if len(expected) > 0 {
		t.Fatalf("expected additional elements: %v", expected)
	}

	if !ok {
		t.FailNow()
	}
}

func TestMsgRingBuffer_seqRange(t *testing.T) {
	var rb *messageRingBuffer
	now := time.Date(2025, time.July, 18, 11, 53, 0, 0, time.UTC)

	rb = newMessageRingBuffer(3)

	rb.Append(nil, now)
	assertSeq2(t, rb.seqRangeUp(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{1, &messageRingBufferItem{msg: nil, t: now}},
	})
	assertSeq2(t, rb.seqRangeDown(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{1, &messageRingBufferItem{msg: nil, t: now}},
	})

	rb.Append(nil, now.Add(1*time.Minute))
	assertSeq2(t, rb.seqRangeUp(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{1, &messageRingBufferItem{msg: nil, t: now}},
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
	})
	assertSeq2(t, rb.seqRangeDown(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
		{1, &messageRingBufferItem{msg: nil, t: now}},
	})

	rb.Append(nil, now.Add(2*time.Minute))
	assertSeq2(t, rb.seqRangeUp(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{1, &messageRingBufferItem{msg: nil, t: now}},
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
		{3, &messageRingBufferItem{msg: nil, t: now.Add(2 * time.Minute)}},
	})
	assertSeq2(t, rb.seqRangeDown(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{3, &messageRingBufferItem{msg: nil, t: now.Add(2 * time.Minute)}},
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
		{1, &messageRingBufferItem{msg: nil, t: now}},
	})

	rb.Append(nil, now.Add(3*time.Minute))
	assertSeq2(t, rb.seqRangeUp(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
		{3, &messageRingBufferItem{msg: nil, t: now.Add(2 * time.Minute)}},
		{4, &messageRingBufferItem{msg: nil, t: now.Add(3 * time.Minute)}},
	})
	assertSeq2(t, rb.seqRangeDown(1, rb.cur), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{4, &messageRingBufferItem{msg: nil, t: now.Add(3 * time.Minute)}},
		{3, &messageRingBufferItem{msg: nil, t: now.Add(2 * time.Minute)}},
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
	})

	assertSeq2(t, rb.seqRangeUp(1, 1), nil)
	assertSeq2(t, rb.seqRangeUp(2, 2), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
	})
	assertSeq2(t, rb.seqRangeUp(3, 3), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{3, &messageRingBufferItem{msg: nil, t: now.Add(2 * time.Minute)}},
	})
	assertSeq2(t, rb.seqRangeUp(4, 4), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{4, &messageRingBufferItem{msg: nil, t: now.Add(3 * time.Minute)}},
	})

	assertSeq2(t, rb.seqRangeUp(2, 3), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
		{3, &messageRingBufferItem{msg: nil, t: now.Add(2 * time.Minute)}},
	})
	assertSeq2(t, rb.seqRangeDown(2, 3), []struct {
		k uint64
		v *messageRingBufferItem
	}{
		{3, &messageRingBufferItem{msg: nil, t: now.Add(2 * time.Minute)}},
		{2, &messageRingBufferItem{msg: nil, t: now.Add(1 * time.Minute)}},
	})
}
