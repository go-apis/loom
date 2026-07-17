// Package gblob implements loom.BlobStore on Google Cloud Storage.
// Upload sessions are GCS resumable uploads: the runtime initiates the
// session server-side and hands the session URI to the browser, which
// PUTs chunks directly to storage — bytes never transit the service.
// Finalized objects reach the domain through the bucket's Pub/Sub
// notifications: run Watch with the notification subscription and
// loom's FinalizeUpload, and every finished upload dispatches its
// schema-declared `on uploaded` command.
//
// Wiring (once per bucket):
//
//	gcloud storage buckets notifications create gs://BUCKET \
//	  --topic=loom-uploads --event-types=OBJECT_FINALIZE
//	gcloud pubsub subscriptions create loom-uploads-SERVICE --topic=loom-uploads
//
// and a CORS rule allowing PUT from the UI origin. A lifecycle rule
// that deletes incomplete resumable sessions after a day keeps
// abandoned uploads from accumulating.
package gblob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"cloud.google.com/go/pubsub/v2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/go-apis/loom"
)

const scope = "https://www.googleapis.com/auth/devstorage.read_write"

type Config struct {
	Bucket string
	// Endpoint overrides the storage base URL (tests, emulators).
	// Default https://storage.googleapis.com.
	Endpoint string
	// TokenSource overrides application default credentials.
	TokenSource oauth2.TokenSource
	// HTTPClient bypasses auth entirely (tests against a fake endpoint).
	HTTPClient *http.Client
	Logger     *slog.Logger
}

type Store struct {
	bucket   string
	endpoint string
	http     *http.Client
	log      *slog.Logger
}

func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gblob: Bucket is required")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "https://storage.googleapis.com"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	client := cfg.HTTPClient
	if client == nil {
		ts := cfg.TokenSource
		if ts == nil {
			var err error
			ts, err = google.DefaultTokenSource(ctx, scope)
			if err != nil {
				return nil, fmt.Errorf("gblob: credentials: %w", err)
			}
		}
		client = oauth2.NewClient(ctx, ts)
	}
	return &Store{bucket: cfg.Bucket, endpoint: cfg.Endpoint, http: client, log: cfg.Logger}, nil
}

// CreateUpload initiates a resumable session. The returned URL is the
// session URI — it authenticates itself, so the browser PUTs chunks to
// it with no further credentials; Origin binds the session's CORS.
func (s *Store) CreateUpload(ctx context.Context, init loom.UploadInit) (*loom.UploadSession, error) {
	body, err := json.Marshal(map[string]any{
		"name":        init.Key,
		"contentType": init.ContentType,
		"metadata":    init.Metadata,
	})
	if err != nil {
		return nil, err
	}
	u := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?uploadType=resumable&name=%s",
		s.endpoint, url.PathEscape(s.bucket), url.QueryEscape(init.Key))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	if init.ContentType != "" {
		req.Header.Set("X-Upload-Content-Type", init.ContentType)
	}
	if init.Size > 0 {
		req.Header.Set("X-Upload-Content-Length", strconv.FormatInt(init.Size, 10))
	}
	if init.Origin != "" {
		req.Header.Set("Origin", init.Origin)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, apiErr("create upload", resp)
	}
	loc := resp.Header.Get("Location")
	if loc == "" {
		return nil, fmt.Errorf("gblob: create upload: no session URI in response")
	}
	return &loom.UploadSession{URL: loc, Protocol: loom.ProtocolGCSResumable}, nil
}

func (s *Store) Stat(ctx context.Context, key string) (*loom.BlobInfo, error) {
	resp, err := s.do(ctx, http.MethodGet, s.objectURL(key), nil)
	if err != nil {
		return nil, err
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, apiErr("stat "+key, resp)
	}
	var obj struct {
		Size        string            `json:"size"` // the JSON API renders int64 as string
		ContentType string            `json:"contentType"`
		Metadata    map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return nil, fmt.Errorf("gblob: stat %s: %w", key, err)
	}
	size, _ := strconv.ParseInt(obj.Size, 10, 64)
	if obj.Metadata == nil {
		obj.Metadata = map[string]string{}
	}
	return &loom.BlobInfo{
		Key:         key,
		Name:        obj.Metadata["loom_name"],
		ContentType: obj.ContentType,
		Size:        size,
		Metadata:    obj.Metadata,
	}, nil
}

func (s *Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.do(ctx, http.MethodGet, s.objectURL(key)+"?alt=media", nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer drain(resp)
		return nil, apiErr("open "+key, resp)
	}
	return resp.Body, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	resp, err := s.do(ctx, http.MethodDelete, s.objectURL(key), nil)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return apiErr("delete "+key, resp)
	}
	return nil
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string) error {
	pageToken := ""
	for {
		u := fmt.Sprintf("%s/storage/v1/b/%s/o?prefix=%s&fields=items(name),nextPageToken",
			s.endpoint, url.PathEscape(s.bucket), url.QueryEscape(prefix))
		if pageToken != "" {
			u += "&pageToken=" + url.QueryEscape(pageToken)
		}
		resp, err := s.do(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		var list struct {
			Items         []struct{ Name string } `json:"items"`
			NextPageToken string                  `json:"nextPageToken"`
		}
		if resp.StatusCode != http.StatusOK {
			defer drain(resp)
			return apiErr("list "+prefix, resp)
		}
		err = json.NewDecoder(resp.Body).Decode(&list)
		drain(resp)
		if err != nil {
			return fmt.Errorf("gblob: list %s: %w", prefix, err)
		}
		for _, item := range list.Items {
			if err := s.Delete(ctx, item.Name); err != nil {
				return err
			}
		}
		if list.NextPageToken == "" {
			return nil
		}
		pageToken = list.NextPageToken
	}
}

// Watch consumes the bucket's OBJECT_FINALIZE notifications from a
// Pub/Sub subscription and hands each finished object to finalize —
// pass loom's Client.FinalizeUpload. Errors nack for redelivery, so a
// briefly-down service loses nothing; objects that aren't this
// service's uploads (loom.ErrForeignObject) are acked and skipped.
// Blocks until ctx is done.
func (s *Store) Watch(ctx context.Context, projectID, subscription string, finalize func(ctx context.Context, key string) error) error {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()
	sub := client.Subscriber(subscription)
	return sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		if m.Attributes["eventType"] != "OBJECT_FINALIZE" || m.Attributes["bucketId"] != s.bucket {
			m.Ack()
			return
		}
		key := m.Attributes["objectId"]
		switch err := finalize(ctx, key); {
		case err == nil:
			m.Ack()
		case errors.Is(err, loom.ErrForeignObject):
			m.Ack()
		default:
			s.log.WarnContext(ctx, "gblob: finalize nack", "key", key, "error", err)
			m.Nack()
		}
	})
}

// --- plumbing ---

func (s *Store) objectURL(key string) string {
	return fmt.Sprintf("%s/storage/v1/b/%s/o/%s", s.endpoint, url.PathEscape(s.bucket), url.PathEscape(key))
}

func (s *Store) do(ctx context.Context, method, u string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	return s.http.Do(req)
}

func apiErr(op string, resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return fmt.Errorf("gblob: %s: %s: %s", op, resp.Status, raw)
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
}
