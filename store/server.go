package store

// Server represents another node in the cluster.
type Server struct {
	ID   string `json:"id,omitempty"`
	Addr string `json:"addr,omitempty"`
}

// Servers is a set of Servers.
type Servers []*Server

func (s Servers) Less(i, j int) bool { return s[i].ID < s[j].ID }
func (s Servers) Len() int           { return len(s) }
func (s Servers) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
