package http

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"path"
	"strconv"
	"strings"
)

// Handler pattern contained here created by github.com/Merovius
// See https://blog.merovius.de/2017/06/18/how-not-to-use-an-http-router.html

type executeHandler struct{}

func (h *executeHandler) Handler(connID uint64, s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats.Add(numExecutions, 1)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		if !s.CheckRequestPerm(r, PermExecute) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleExecute(connID, w, r)
	})
}

type queryHandler struct{}

func (h *queryHandler) Handler(connID uint64, s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats.Add(numQueries, 1)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		if !s.CheckRequestPerm(r, PermQuery) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method != "GET" && r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleQuery(connID, w, r)
	})
}

type backupHandler struct{}

func (h *backupHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats.Add(numBackups, 1)
		if !s.CheckRequestPerm(r, PermBackup) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleBackup(w, r)
	})
}

type loadHandler struct{}

func (h *loadHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats.Add(numLoad, 1)
		if !s.CheckRequestPerm(r, PermLoad) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleLoad(w, r)
	})
}

type connectionsHandler struct {
	executeH executeHandler
	queryH   queryHandler
}

func (h *connectionsHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats.Add(numConnections, 1)
		if !s.CheckRequestPerm(r, PermConnections) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var head string
		head, r.URL.Path = shiftPath(r.URL.Path)
		if head == "" {
			if r.Method == "POST" {
				id, err := s.createConnection(w, r)
				if err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}

				w.Header().Set("Location", s.FormConnectionURL(r, id))
				w.WriteHeader(http.StatusCreated)
			} else {
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		} else {
			id, err := strconv.ParseUint(head, 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
			}
			if r.Method == "DELETE" {
				if err := s.deleteConnection(id); err != nil {
					if err == ErrConnectionNotFound {
						http.Error(w, err.Error(), http.StatusNotFound)
					} else {
						http.Error(w, err.Error(), http.StatusServiceUnavailable)
					}
				}
				w.WriteHeader(http.StatusNoContent)
			} else {
				switch head {
				case "execute":
					h.executeH.Handler(id, s).ServeHTTP(w, r)
				case "query":
					h.queryH.Handler(id, s).ServeHTTP(w, r)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}
		}
	})
}

type databaseHandler struct {
	executeH     executeHandler
	queryH       queryHandler
	backupH      backupHandler
	loadH        loadHandler
	connectionsH connectionsHandler
}

func (h *databaseHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var head string
		head, r.URL.Path = shiftPath(r.URL.Path)
		switch head {
		case "execute":
			h.executeH.Handler(defaultConnID, s).ServeHTTP(w, r)
		case "query":
			h.queryH.Handler(defaultConnID, s).ServeHTTP(w, r)
		case "backup":
			h.backupH.Handler(s).ServeHTTP(w, r)
		case "load":
			h.loadH.Handler(s).ServeHTTP(w, r)
		case "connections":
			h.connectionsH.Handler(s).ServeHTTP(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

type joinHandler struct{}

func (h *joinHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.CheckRequestPerm(r, PermJoin) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleJoin(w, r)
	})
}

type removeHandler struct{}

func (h *removeHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.CheckRequestPerm(r, PermRemove) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method != "DELETE" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleRemove(w, r)
	})
}

type statusHandler struct{}

func (h *statusHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		if !s.CheckRequestPerm(r, PermStatus) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleStatus(w, r)
	})
}

type varsHandler struct{}

func (h *varsHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.Expvar {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if !s.CheckRequestPerm(r, PermStatus) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		fmt.Fprintf(w, "{\n")
		first := true
		expvar.Do(func(kv expvar.KeyValue) {
			if !first {
				fmt.Fprintf(w, ",\n")
			}
			first = false
			fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
		})
		fmt.Fprintf(w, "\n}\n")
	})
}

type pprofHandler struct{}

func (h *pprofHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.Pprof {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if !s.CheckRequestPerm(r, PermStatus) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "cmdline":
			pprof.Cmdline(w, r)
		case "profile":
			pprof.Profile(w, r)
		case "symbol":
			pprof.Symbol(w, r)
		default:
			pprof.Index(w, r)
		}
	})
}

type debugHandler struct {
	varsH  varsHandler
	pprofH pprofHandler
}

func (h *debugHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var head string
		head, r.URL.Path = shiftPath(r.URL.Path)
		switch head {
		case "vars":
			h.varsH.Handler(s).ServeHTTP(w, r)
		case "pprof":
			h.pprofH.Handler(s).ServeHTTP(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

type rootHandler struct {
	dbH     databaseHandler
	joinH   joinHandler
	removeH removeHandler
	statusH statusHandler
	debugH  debugHandler
}

func (h *rootHandler) Handler(s *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.addBuildVersion(w)

		if s.credentialStore != nil {
			username, password, ok := r.BasicAuth()
			if !ok || !s.credentialStore.Check(username, password) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		var head string
		head, r.URL.Path = shiftPath(r.URL.Path)
		switch head {
		case "db":
			h.dbH.Handler(s).ServeHTTP(w, r)
		case "join":
			h.joinH.Handler(s).ServeHTTP(w, r)
		case "remove":
			h.removeH.Handler(s).ServeHTTP(w, r)
		case "status":
			h.statusH.Handler(s).ServeHTTP(w, r)
		case "debug":
			h.debugH.Handler(s).ServeHTTP(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func shiftPath(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}
