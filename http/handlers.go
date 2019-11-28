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

const (
	majorActionDB     = "db"
	majorActionJoin   = "join"
	majorActionRemove = "remove"
	majorActionStatus = "status"
	majorActionDebug  = "debug"

	subActionDBExecute    = "execute"
	subActionDBQuery      = "query"
	subActionDBBackup     = "backup"
	subActionDBLoad       = "load"
	subActionDBConnection = "connections"

	subActionConnectionExcute = "execute"
	subActionConnectionQuery  = "query"

	subActionDebugVars  = "vars"
	subActionDebugPprof = "pprof"

	subActionDebugPprofCmdline = "cmdline"
	subActionDebugPprofProfile = "profile"
	subActionDebugPprofSymbol  = "symbol"
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

		if r.Method != http.MethodPost {
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

		if r.Method != http.MethodGet && r.Method != http.MethodPost {
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

		if r.Method != http.MethodGet {
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

		if r.Method != http.MethodPost {
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

		head := getConnectionID(r.URL.Path)
		if head == "" {
			if r.Method == http.MethodPost {
				s.createConnection(w, r)

			} else {
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		} else {
			id, err := strconv.ParseUint(head, 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
			}
			if r.Method == http.MethodDelete {
				if err := s.deleteConnection(id); err != nil {
					if err == ErrConnectionNotFound {
						http.Error(w, err.Error(), http.StatusNotFound)
					} else {
						http.Error(w, err.Error(), http.StatusServiceUnavailable)
					}
				}
				w.WriteHeader(http.StatusNoContent)
			} else {
				head = getConnectionSubaction(r.URL.Path)
				switch head {
				case subActionConnectionExcute:
					h.executeH.Handler(id, s).ServeHTTP(w, r)
				case subActionConnectionQuery:
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
		head := getSubAction(r.URL.Path)
		switch head {
		case subActionDBExecute:
			h.executeH.Handler(defaultConnID, s).ServeHTTP(w, r)
		case subActionDBQuery:
			h.queryH.Handler(defaultConnID, s).ServeHTTP(w, r)
		case subActionDBBackup:
			h.backupH.Handler(s).ServeHTTP(w, r)
		case subActionDBLoad:
			h.loadH.Handler(s).ServeHTTP(w, r)
		case subActionDBConnection:
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

		if r.Method != http.MethodPost {
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

		if r.Method != http.MethodDelete {
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

		if r.Method != http.MethodGet {
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
		head := getDebugPprofSubaction(r.URL.Path)
		switch head {
		case subActionDebugPprofCmdline:
			pprof.Cmdline(w, r)
		case subActionDebugPprofProfile:
			pprof.Profile(w, r)
		case subActionDebugPprofSymbol:
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
		head := getSubAction(r.URL.Path)
		switch head {
		case subActionDebugVars:
			h.varsH.Handler(s).ServeHTTP(w, r)
		case subActionDebugPprof:
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

		head := getMajorAction(r.URL.Path)
		switch head {
		case majorActionDB:
			h.dbH.Handler(s).ServeHTTP(w, r)
		case majorActionJoin:
			h.joinH.Handler(s).ServeHTTP(w, r)
		case majorActionRemove:
			h.removeH.Handler(s).ServeHTTP(w, r)
		case majorActionStatus:
			h.statusH.Handler(s).ServeHTTP(w, r)
		case majorActionDebug:
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

func getMajorAction(path string) string {
	head, _ := shiftPath(path)
	return head
}

func getSubAction(path string) string {
	//shift path two times, first for majory action, second for sub actions
	_, path = shiftPath(path)
	head, path := shiftPath(path)
	return head
}

func getConnectionID(path string) string {
	//shift path two times, first for majory action, second for sub actions
	_, path = shiftPath(path)
	subAction, path := shiftPath(path)
	if subAction != subActionDBConnection {
		return ""
	}
	connectionID, path := shiftPath(path)

	return connectionID
}

func getConnectionSubaction(path string) string {
	//shift path two times, first for majory action, second for sub actions
	_, path = shiftPath(path)
	subAction, path := shiftPath(path)
	if subAction != subActionDBConnection {
		return ""
	}
	_, path = shiftPath(path)
	connectionAction, path := shiftPath(path)

	return connectionAction
}

func getDebugPprofSubaction(path string) string {
	//shift path two times, first for majory action, second for sub actions
	_, path = shiftPath(path)
	subAction, path := shiftPath(path)
	if subAction != subActionDebugPprof {
		return ""
	}
	action, path := shiftPath(path)

	return action
}
