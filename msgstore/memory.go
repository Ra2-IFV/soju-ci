package msgstore

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"time"

	"git.sr.ht/~sircmpwn/go-bare"
	"gopkg.in/irc.v4"

	"codeberg.org/emersion/soju/database"
	"codeberg.org/emersion/soju/xirc"
)

const messageRingBufferCap = 4096

type memoryMsgID struct {
	Seq bare.Uint
}

func (memoryMsgID) msgIDType() msgIDType {
	return msgIDMemory
}

func parseMemoryMsgID(s string) (netID int64, entity string, seq uint64, err error) {
	var id memoryMsgID
	netID, entity, err = ParseMsgID(s, &id)
	if err != nil {
		return 0, "", 0, err
	}
	return netID, entity, uint64(id.Seq), nil
}

func formatMemoryMsgID(netID int64, entity string, seq uint64) string {
	id := memoryMsgID{bare.Uint(seq)}
	return formatMsgID(netID, entity, &id)
}

type ringBufferKey struct {
	networkID int64
	entity    string
}

func IsMemoryStore(store Store) bool {
	_, ok := store.(*memoryMessageStore)
	return ok
}

type memoryMessageStore struct {
	buffers map[ringBufferKey]*messageRingBuffer
}

var (
	_ Store            = (*memoryMessageStore)(nil)
	_ ChatHistoryStore = (*memoryMessageStore)(nil)
)

func NewMemoryStore() *memoryMessageStore {
	return &memoryMessageStore{
		buffers: make(map[ringBufferKey]*messageRingBuffer),
	}
}

func (ms *memoryMessageStore) Close() error {
	ms.buffers = nil
	return nil
}

func (ms *memoryMessageStore) get(network *database.Network, entity string) *messageRingBuffer {
	k := ringBufferKey{networkID: network.ID, entity: entity}
	if rb, ok := ms.buffers[k]; ok {
		return rb
	}
	rb := newMessageRingBuffer(messageRingBufferCap)
	ms.buffers[k] = rb
	return rb
}

func (ms *memoryMessageStore) LastMsgID(network *database.Network, entity string, t time.Time) (string, error) {
	var seq uint64
	k := ringBufferKey{networkID: network.ID, entity: entity}
	if rb, ok := ms.buffers[k]; ok {
		seq = rb.cur
	}
	return formatMemoryMsgID(network.ID, entity, seq), nil
}

func (ms *memoryMessageStore) Append(network *database.Network, entity string, msg *irc.Message) (string, error) {
	switch msg.Command {
	case "PRIVMSG", "NOTICE", "TAGMSG":
		// Only append these messages, because LoadLatestID shouldn't return
		// other kinds of message.
	default:
		return "", nil
	}

	k := ringBufferKey{networkID: network.ID, entity: entity}
	rb, ok := ms.buffers[k]
	if !ok {
		rb = newMessageRingBuffer(messageRingBufferCap)
		ms.buffers[k] = rb
	}

	var t time.Time
	if tag, ok := msg.Tags["time"]; ok {
		var err error
		t, err = time.Parse(xirc.ServerTimeLayout, string(tag))
		if err != nil {
			return "", fmt.Errorf("failed to parse message time tag: %v", err)
		}
	} else {
		t = time.Now()
	}

	seq := rb.Append(msg, t)
	return formatMemoryMsgID(network.ID, entity, seq), nil
}

func (ms *memoryMessageStore) LoadLatestID(ctx context.Context, id string, options *LoadMessageOptions) ([]*irc.Message, error) {
	_, _, seq, err := parseMemoryMsgID(id)
	if err != nil {
		return nil, err
	}

	k := ringBufferKey{networkID: options.Network.ID, entity: options.Entity}
	rb, ok := ms.buffers[k]
	if !ok {
		return nil, nil
	}

	return rb.LoadLatestSeq(seq, options.Limit)
}

func (ms *memoryMessageStore) ListTargets(ctx context.Context, network *database.Network, start time.Time, end time.Time, limit int, events bool) ([]ChatHistoryTarget, error) {
	ascOrder := start.Before(end)
	if !ascOrder {
		start, end = end, start
	}

	var targets []ChatHistoryTarget
	for k, rb := range ms.buffers {
		if k.networkID != network.ID {
			continue
		}
		if rb.cur <= 1 {
			// shouldn't happen
			continue
		}
		latestSeq := rb.cur - 1
		latestT := rb.buf[latestSeq%rb.cap()].t
		if latestT.Before(start) || end.Before(latestT) {
			continue
		}
		// TODO if !options.Events, only add targets if [start;end] has a NOTICE/PRIVMSG
		targets = append(targets, ChatHistoryTarget{
			Name:          k.entity,
			LatestMessage: latestT,
		})
	}

	sort.Slice(targets, func(i, j int) bool {
		t1, t2 := targets[i].LatestMessage, targets[j].LatestMessage
		if ascOrder {
			return t1.Before(t2)
		} else {
			return !t1.Before(t2)
		}
	})

	if len(targets) >= limit {
		targets = targets[:limit]
	}

	return targets, nil
}

func (ms *memoryMessageStore) LoadAfterTime(ctx context.Context, start time.Time, end time.Time, options *LoadMessageOptions) ([]*irc.Message, error) {
	// this function expects start.Before(end)

	k := ringBufferKey{networkID: options.Network.ID, entity: options.Entity}
	rb := ms.buffers[k]
	if rb == nil {
		return nil, nil
	}

	startSeq, ok := rb.firstAfterTime(start)
	if !ok {
		return nil, nil
	}
	endSeq, ok := rb.lastBeforeTime(end)
	if !ok {
		// shouldn't happen
		return nil, nil
	}

	msgCount := min(int(endSeq-startSeq+1), options.Limit)
	msgs := make([]*irc.Message, 0, msgCount)
	for _, item := range rb.seqRangeUp(startSeq, endSeq) {
		if len(msgs) >= msgCount {
			break
		}
		if !options.Events && item.msg.Command == "TAGMSG" {
			continue
		}
		msgs = append(msgs, item.msg)
	}

	return msgs, nil
}

func (ms *memoryMessageStore) LoadBeforeTime(ctx context.Context, start time.Time, end time.Time, options *LoadMessageOptions) ([]*irc.Message, error) {
	// this function expects end.Before(start)

	k := ringBufferKey{networkID: options.Network.ID, entity: options.Entity}
	rb := ms.buffers[k]
	if rb == nil {
		return nil, nil
	}

	startSeq, ok := rb.lastBeforeTime(start)
	if !ok {
		return nil, nil
	}
	endSeq, ok := rb.firstAfterTime(end)
	if !ok {
		// shouldn't happen
		return nil, nil
	}

	msgCount := min(int(startSeq-endSeq+1), options.Limit)
	msgs := make([]*irc.Message, msgCount)
	i := msgCount
	for _, item := range rb.seqRangeDown(endSeq, startSeq) {
		if i == 0 {
			break
		}
		if !options.Events && item.msg.Command == "TAGMSG" {
			continue
		}
		i--
		msgs[i] = item.msg
	}

	return msgs[i:], nil
}

type messageRingBuffer struct {
	buf []messageRingBufferItem
	cur uint64
}

type messageRingBufferItem struct {
	msg *irc.Message
	t   time.Time
}

func newMessageRingBuffer(capacity int) *messageRingBuffer {
	return &messageRingBuffer{
		buf: make([]messageRingBufferItem, capacity),
		cur: 1,
	}
}

func (rb *messageRingBuffer) cap() uint64 {
	return uint64(len(rb.buf))
}

func (rb *messageRingBuffer) Append(msg *irc.Message, t time.Time) uint64 {
	seq := rb.cur
	i := int(seq % rb.cap())
	rb.buf[i] = messageRingBufferItem{msg: msg, t: t}
	rb.cur++
	return seq
}

func (rb *messageRingBuffer) LoadLatestSeq(seq uint64, limit int) ([]*irc.Message, error) {
	if seq > rb.cur {
		return nil, fmt.Errorf("loading messages from sequence number (%v) greater than current (%v)", seq, rb.cur)
	} else if seq == rb.cur {
		return nil, nil
	}

	// The query excludes the message with the sequence number seq
	msgCount := min(int(rb.cur-seq-1), int(rb.cap()), limit)
	l := make([]*irc.Message, 0, msgCount)
	for _, item := range rb.seqRangeUp(seq, rb.cur) {
		if len(l) >= limit {
			break
		}
		if item.msg.Command == "TAGMSG" {
			continue
		}
		l = append(l, item.msg)
	}

	return l, nil
}

// seqRangeUp iterates through the largest subset of [start;end] available in rb
// in *increasing* order.
// The iterator can be cached, but the ring buffer may not be appended to during
// an iteration.
func (rb *messageRingBuffer) seqRangeUp(start, end uint64) iter.Seq2[uint64, *messageRingBufferItem] {
	return func(yield func(uint64, *messageRingBufferItem) bool) {
		cap := rb.cap()
		start = max(start, 1)
		if cap < rb.cur {
			start = max(start, rb.cur-cap)
		}
		end = min(end, rb.cur-1)
		for seq := start; seq <= end; seq++ {
			if !yield(seq, &rb.buf[seq%cap]) {
				break
			}
		}
	}
}

// seqRangeDown iterates through the largest subset of [start;end] available in
// rb in *decreasing* order.
// The iterator can be cached, but the ring buffer may not be appended to during
// an iteration.
func (rb *messageRingBuffer) seqRangeDown(end, start uint64) iter.Seq2[uint64, *messageRingBufferItem] {
	return func(yield func(uint64, *messageRingBufferItem) bool) {
		cap := rb.cap()
		start = min(start, rb.cur-1)
		end = max(end, 1)
		if cap < rb.cur {
			end = max(end, rb.cur-cap)
		}
		for seq := start; seq >= end; seq-- {
			if !yield(seq, &rb.buf[seq%cap]) {
				break
			}
		}
	}
}

// firstAfterTime returns the sequence number of the first message whose
// timestamp is strictly after t.
func (rb *messageRingBuffer) firstAfterTime(t time.Time) (seq uint64, found bool) {
	cap := rb.cap()
	nItems := cap
	if rb.cur <= cap {
		nItems = rb.cur - 1
	}
	idx, _ := sort.Find(int(nItems), func(i int) int {
		j := (rb.cur - nItems + uint64(i)) % cap
		// bitwise or: so that 0 is mapped to 1, to avoid messages whose
		// timestamps are equal to t
		return t.Compare(rb.buf[j].t) | 1
	})
	return rb.cur - nItems + uint64(idx), idx < int(nItems)
}

// firstBeforeTime returns the sequence number of the last message whose
// timestamp is strictly before t.
func (rb *messageRingBuffer) lastBeforeTime(t time.Time) (seq uint64, found bool) {
	cap := rb.cap()
	nItems := cap
	if rb.cur <= cap {
		nItems = rb.cur - 1
	}
	idx, _ := sort.Find(int(nItems), func(i int) int {
		j := (rb.cur - nItems + uint64(i)) % cap
		// no bitwise or here (unlike in firstAfterTime) because we want
		// the message before the earliest one that may have timestamp
		// t, so that we can idx-1 to get the one strictly before.
		return t.Compare(rb.buf[j].t)
	})
	seq = rb.cur - nItems + uint64(idx)
	if idx == 0 {
		// seq's timestamp is after t, and seq is the earliest message
		return 0, false
	}
	return seq - 1, true
}
