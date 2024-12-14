// Package xirc contains an extended IRC library.
package xirc

import (
	"sort"
	"strings"

	"gopkg.in/irc.v4"
)

// Raw returns a deterministic representation of an IRC message.
//
// This is basically irc.Message.String() but with the tag map
// output in a deterministic order.
func Raw(m *irc.Message) string {
	if len(m.Tags) == 0 {
		return m.String()
	}
	t := m.Tags
	m.Tags = nil
	var sb strings.Builder
	sb.WriteByte('@')
	keys := make([]string, 0)
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		v := t[k]
		if i > 0 {
			sb.WriteByte(';')
		}
		sb.WriteString(k)
		if v != "" {
			sb.WriteByte('=')
			sb.WriteString(irc.EncodeTagValue(v))
		}
	}
	sb.WriteString(m.Tags.String())
	sb.WriteByte(' ')
	sb.WriteString(m.String())
	m.Tags = t
	return sb.String()
}
