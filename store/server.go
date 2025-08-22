package store

// Server represents another node in the cluster.
type Server struct {
	ID       string `json:"id,omitempty"`
	Addr     string `json:"addr,omitempty"`
	Suffrage string `json:"suffrage,omitempty"`
}

// Equal returns whether the two servers are identical.
func (s *Server) Equal(other *Server) bool {
	if s == nil || other == nil {
		return false
	}
	return s.ID == other.ID && s.Addr == other.Addr && s.Suffrage == other.Suffrage
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

// IsReadOnly returns whether the given node, as specified by its Raft ID,
// is a read-only (non-voting) node. If no node is found with the given ID
// then found will be false.
func (s Servers) IsReadOnly(id string) (readOnly bool, found bool) {
	readOnly = false
	found = false

	if s == nil || id == "" {
		return
	}

	for _, n := range s {
		if n != nil && n.ID == id {
			readOnly = n.Suffrage == "Nonvoter"
			found = true
			return
		}
	}
	return
}

// Contains returns whether the given node, as specified by its Raft ID,
// is a member of the set of servers.
func (s Servers) Contains(id string) bool {
	if s == nil || id == "" {
		return false
	}

	for _, n := range s {
		if n != nil && n.ID == id {
			return true
		}
	}
	return false
}

// Voters returns a new slice of servers, only containing the voting nodes.
func (s Servers) Voters() Servers {
	if s == nil {
		return nil
	}

	var voters Servers
	for _, n := range s {
		if n != nil && n.Suffrage != "Nonvoter" {
			voters = append(voters, n)
		}
	}
	return voters
}

// NonVoters returns a new slice of servers, only containing the non-voting nodes.
func (s Servers) NonVoters() Servers {
	if s == nil {
		return nil
	}

	var nonVoters Servers
	for _, n := range s {
		if n != nil && n.Suffrage == "Nonvoter" {
			nonVoters = append(nonVoters, n)
		}
	}
	return nonVoters
}

// Addrs returns a slice of addresses from the servers.
func (s Servers) Addrs() []string {
	if s == nil {
		return nil
	}

	addrs := make([]string, 0, len(s))
	for _, server := range s {
		if server != nil {
			addrs = append(addrs, server.Addr)
		}
	}
	return addrs
}

func (s Servers) Less(i, j int) bool { return s[i].ID < s[j].ID }
func (s Servers) Len() int           { return len(s) }
func (s Servers) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
