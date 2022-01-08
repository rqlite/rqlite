package store

// Server represents another node in the cluster.
type Server struct {
	ID       string `json:"id,omitempty"`
	Addr     string `json:"addr,omitempty"`
	Suffrage string `json:"suffrage,omitempty"`
}

// NewServer returns an initialized Server.
func NewServer(id, addr string, voter bool) *Server {
	v := "voter"
	if !voter {
		v = "Nonvoter"
	}
	return &Server{
		ID:       id,
		Addr:     addr,
		Suffrage: v,
	}
}

// Servers is a set of Servers.
type Servers []*Server

func (s Servers) Less(i, j int) bool { return s[i].ID < s[j].ID }
func (s Servers) Len() int           { return len(s) }
func (s Servers) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
