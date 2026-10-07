package event

// countdown is how many later records of its pod an entry waits for the record
// that takes it.
const countdown = 1000

// Auth is what a "connection authenticated" record says about a session.
type Auth struct {
	Identity string
	Method   string
}

// Sessions holds the Auth of each session that is authenticated but not yet
// logged in or failed, in one table per pod. It has no lock.
type Sessions struct {
	pods map[string]map[string]*entry
}

type entry struct {
	auth Auth
	left int
}

// NewSessions returns an empty session table.
func NewSessions() *Sessions {
	return &Sessions{pods: map[string]map[string]*entry{}}
}

// Add puts the Auth of a session in its pod's table.
func (s *Sessions) Add(pod, session string, auth Auth) {
	if s.pods[pod] == nil {
		s.pods[pod] = map[string]*entry{}
	}
	s.pods[pod][session] = &entry{auth: auth, left: countdown}
}

// Take removes a session from its pod's table and returns its Auth. It
// reports false when the session is not in the table.
func (s *Sessions) Take(pod, session string) (Auth, bool) {
	e, ok := s.pods[pod][session]
	if !ok {
		return Auth{}, false
	}
	delete(s.pods[pod], session)
	return e.auth, true
}

// CountDown counts every entry of a pod down by one and removes the entries
// that reach zero.
func (s *Sessions) CountDown(pod string) {
	for session, e := range s.pods[pod] {
		if e.left--; e.left == 0 {
			delete(s.pods[pod], session)
		}
	}
}
