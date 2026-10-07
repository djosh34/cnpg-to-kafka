package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSessions(t *testing.T) {
	auth := Auth{Identity: "included", Method: "scram-sha-256"}
	cases := []struct {
		name string
		// countDowns of the pod between Add and Take.
		countDowns int
		// takePod and takeSession are what Take asks for.
		takePod, takeSession string
		ok                   bool
	}{
		{name: "take at once", takePod: "ns/cnpg-1", takeSession: "s1", ok: true},
		{name: "take before zero", countDowns: countdown - 1, takePod: "ns/cnpg-1", takeSession: "s1", ok: true},
		{name: "removed at zero", countDowns: countdown, takePod: "ns/cnpg-1", takeSession: "s1"},
		{name: "other session", takePod: "ns/cnpg-1", takeSession: "s2"},
		{name: "same session on another pod", takePod: "ns/cnpg-2", takeSession: "s1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sessions := NewSessions()
			sessions.Add("ns/cnpg-1", "s1", auth)
			for range c.countDowns {
				sessions.CountDown("ns/cnpg-1")
				// Records of another pod do not count this pod's entries down.
				sessions.CountDown("ns/cnpg-2")
			}
			got, ok := sessions.Take(c.takePod, c.takeSession)
			assert.Equal(t, c.ok, ok)
			if c.ok {
				assert.Equal(t, auth, got)
				// Take removes the entry.
				_, ok = sessions.Take(c.takePod, c.takeSession)
				assert.False(t, ok)
			}
		})
	}
}
