package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// DirBlobStore is the dev/test BlobStore: objects in a local directory,
// sessions in memory, and the same chunked-PUT resumable protocol GCS
// speaks (Content-Range chunks, 308 + Range progress, probe with
// `bytes */N`) — so upload client code runs unchanged against local dev.
// Mount it wherever the deployment serves HTTP:
//
//	store := loom.NewDirBlobStore("data/blobs", "http://localhost:8099/blobs")
//	mux.Handle("/blobs/", http.StripPrefix("/blobs", store))
//
// Finalized uploads call the runtime synchronously (loom.New wires
// FinalizeUpload via UploadNotifier), so the `on uploaded` command has
// been dispatched by the time the last chunk's 200 returns.
type DirBlobStore struct {
	dir  string
	base string

	mu       sync.Mutex
	sessions map[string]*dirSession
	finalize func(ctx context.Context, key string) error
}

type dirSession struct {
	init    UploadInit
	written int64
	done    bool
}

func NewDirBlobStore(dir, baseURL string) *DirBlobStore {
	return &DirBlobStore{dir: dir, base: strings.TrimSuffix(baseURL, "/"), sessions: map[string]*dirSession{}}
}

func (s *DirBlobStore) NotifyUploads(finalize func(ctx context.Context, key string) error) {
	s.finalize = finalize
}

func (s *DirBlobStore) CreateUpload(ctx context.Context, init UploadInit) (*UploadSession, error) {
	token := uuid.NewString()
	s.mu.Lock()
	s.sessions[token] = &dirSession{init: init}
	s.mu.Unlock()
	return &UploadSession{URL: s.base + "/" + token, Protocol: ProtocolGCSResumable}, nil
}

// objectPath resolves a key inside the store's directory, refusing
// traversal out of it.
func (s *DirBlobStore) objectPath(key string) (string, error) {
	path := filepath.Join(s.dir, filepath.FromSlash(key))
	root := filepath.Clean(s.dir) + string(filepath.Separator)
	if !strings.HasPrefix(path, root) {
		return "", fmt.Errorf("loom: blob key %q escapes the store", key)
	}
	return path, nil
}

func (s *DirBlobStore) metaPath(objectPath string) string { return objectPath + ".loommeta" }

func (s *DirBlobStore) Stat(ctx context.Context, key string) (*BlobInfo, error) {
	path, err := s.objectPath(key)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	info := &BlobInfo{Key: key, Size: fi.Size(), Metadata: map[string]string{}}
	if raw, err := os.ReadFile(s.metaPath(path)); err == nil {
		var meta struct {
			Name        string            `json:"name"`
			ContentType string            `json:"content_type"`
			Metadata    map[string]string `json:"metadata"`
		}
		if json.Unmarshal(raw, &meta) == nil {
			info.Name, info.ContentType = meta.Name, meta.ContentType
			if meta.Metadata != nil {
				info.Metadata = meta.Metadata
			}
		}
	}
	return info, nil
}

func (s *DirBlobStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	path, err := s.objectPath(key)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (s *DirBlobStore) Delete(ctx context.Context, key string) error {
	path, err := s.objectPath(key)
	if err != nil {
		return err
	}
	_ = os.Remove(s.metaPath(path))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *DirBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	path, err := s.objectPath(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}

// --- the resumable upload endpoint ---

// ServeHTTP handles PUT /{token}: GCS-shaped resumable chunks. CORS is
// wide open — this store is for dev.
func (s *DirBlobStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "PUT, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Range, Content-Type")
	w.Header().Set("Access-Control-Expose-Headers", "Range")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPut {
		http.Error(w, "PUT chunks here", http.StatusMethodNotAllowed)
		return
	}
	token := strings.Trim(r.URL.Path, "/")
	s.mu.Lock()
	sess := s.sessions[token]
	s.mu.Unlock()
	if sess == nil {
		http.Error(w, "unknown upload session", http.StatusNotFound)
		return
	}

	start, end, probe, err := parseContentRange(r.Header.Get("Content-Range"), r.ContentLength)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if probe {
		s.respondProgress(w, r, sess)
		return
	}
	if start != sess.written {
		// out-of-sync chunk: report progress, client resumes from there
		s.respondProgress(w, r, sess)
		return
	}

	part, err := s.partPath(token)
	if err == nil {
		var f *os.File
		f, err = os.OpenFile(part, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			_, err = io.Copy(f, r.Body)
			f.Close()
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sess.written = end + 1
	s.respondProgress(w, r, sess)
}

// respondProgress completes the object when all bytes are in (running
// finalize synchronously), else reports 308 + Range like GCS does.
func (s *DirBlobStore) respondProgress(w http.ResponseWriter, r *http.Request, sess *dirSession) {
	if sess.written < sess.init.Size {
		if sess.written > 0 {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", sess.written-1))
		}
		w.WriteHeader(308)
		return
	}
	if !sess.done {
		if err := s.complete(r.Context(), sess); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sess.done = true
	}
	// finalize is idempotent (the runtime dedups on the object key), so a
	// repeated final chunk or probe lands here harmlessly
	if s.finalize != nil {
		if err := s.finalize(r.Context(), sess.init.Key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": sess.init.Key, "size": sess.init.Size})
}

func (s *DirBlobStore) complete(ctx context.Context, sess *dirSession) error {
	path, err := s.objectPath(sess.init.Key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	token := ""
	s.mu.Lock()
	for t, cand := range s.sessions {
		if cand == sess {
			token = t
		}
	}
	s.mu.Unlock()
	part, err := s.partPath(token)
	if err != nil {
		return err
	}
	if err := os.Rename(part, path); err != nil {
		return err
	}
	meta, err := json.Marshal(map[string]any{
		"name":         sess.init.Name,
		"content_type": sess.init.ContentType,
		"metadata":     sess.init.Metadata,
	})
	if err != nil {
		return err
	}
	return os.WriteFile(s.metaPath(path), meta, 0o644)
}

func (s *DirBlobStore) partPath(token string) (string, error) {
	if token == "" || strings.ContainsAny(token, "/\\.") {
		return "", fmt.Errorf("loom: bad upload token")
	}
	dir := filepath.Join(s.dir, ".uploads")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(dir, token), nil
}

// parseContentRange reads the resumable chunk header: `bytes a-b/N`
// carries data, `bytes */N` probes progress. A missing header means the
// whole object arrives in one PUT.
func parseContentRange(h string, contentLength int64) (start, end int64, probe bool, err error) {
	if h == "" {
		if contentLength < 0 {
			return 0, 0, false, fmt.Errorf("Content-Range or Content-Length required")
		}
		return 0, contentLength - 1, false, nil
	}
	rest, ok := strings.CutPrefix(h, "bytes ")
	if !ok {
		return 0, 0, false, fmt.Errorf("bad Content-Range %q", h)
	}
	span, _, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, 0, false, fmt.Errorf("bad Content-Range %q", h)
	}
	if span == "*" {
		return 0, 0, true, nil
	}
	if _, err := fmt.Sscanf(span, "%d-%d", &start, &end); err != nil || end < start {
		return 0, 0, false, fmt.Errorf("bad Content-Range %q", h)
	}
	return start, end, false, nil
}
