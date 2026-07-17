package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
)

// ErrForeignObject marks a finalize signal for an object that is not one
// of this service's uploads (buckets are often shared): watchers skip it
// instead of retrying it forever.
var ErrForeignObject = errors.New("loom: not an upload object of this service")

// Large files never enter the event log or cross the bus: the runtime
// brokers a resumable upload session against a BlobStore (GCS in
// production, a local directory in dev) and the client sends its chunks
// directly to storage. What the domain sees is the upload's lifecycle,
// as commands the schema declares:
//
//	upload W9 {
//	  on started  -> RequestW9   // optional: session created
//	  on uploaded -> AttachW9    // required: object finalized, verified
//	}
//
// Both are ordinary commands of the enclosing aggregate/record with one
// required `file` field; loom fills the FileRef and dispatches. The
// `uploaded` dispatch is driven by the store's finalize signal (a GCS
// Object Finalize notification, or the dev store's last chunk), never by
// the client claiming it finished — the object is Stat'ed and the
// FileRef records what storage actually holds.
//
// Object keys are stream-prefixed (service/namespace/streamID/…), so
// Shred can erase a stream's files the same way it erases its @pii.

// FileRef is what a `file` schema field holds: a pointer to an object in
// the service's blob store. Events and states carry references; bytes
// stay in storage.
type FileRef struct {
	ID          uuid.UUID `json:"id"`
	Key         string    `json:"key"`
	Name        string    `json:"name,omitempty"`
	ContentType string    `json:"content_type,omitempty"`
	Size        int64     `json:"size"`
}

// UploadInit is what a BlobStore needs to open a resumable session.
type UploadInit struct {
	Key         string
	Name        string // original filename, kept as object metadata
	ContentType string
	Size        int64
	// Origin is the browser origin the session is created for (CORS);
	// stores that don't bind sessions to origins may ignore it.
	Origin   string
	Metadata map[string]string
}

// Upload protocols — the chunk dialect the client must speak against
// the session URL. The domain contract is provider-agnostic; this one
// field is where the storage provider shows through, so clients switch
// on it instead of assuming a dialect.
const (
	// ProtocolGCSResumable: one self-authenticating session URL; PUT
	// chunks with Content-Range, probe/resume via 308 + Range. Spoken by
	// gblob and DirBlobStore.
	ProtocolGCSResumable = "gcs-resumable"
	// ProtocolS3Multipart is reserved for a future S3/R2 store: presigned
	// URL per part + a complete step (needs two extra endpoints — see
	// TODO.md before implementing).
	ProtocolS3Multipart = "s3-multipart"
)

// UploadSession is the client's half of a resumable upload: speak
// Protocol against URL until the store reports completion.
type UploadSession struct {
	URL      string `json:"url"`
	Protocol string `json:"protocol"`
}

// BlobInfo describes a stored object.
type BlobInfo struct {
	Key         string
	Name        string
	ContentType string
	Size        int64
	Metadata    map[string]string
}

// BlobStore is the storage seam behind uploads, mirroring the Bus seam:
// gblob implements it on Google Cloud Storage, DirBlobStore on a local
// directory for dev and tests.
type BlobStore interface {
	// CreateUpload opens a resumable upload session for the object.
	CreateUpload(ctx context.Context, init UploadInit) (*UploadSession, error)
	// Stat describes an object; (nil, nil) means it does not exist.
	Stat(ctx context.Context, key string) (*BlobInfo, error)
	// Open streams an object's bytes.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete removes one object; deleting a missing object is not an error.
	Delete(ctx context.Context, key string) error
	// DeletePrefix removes every object under prefix (Shred's lever).
	DeletePrefix(ctx context.Context, prefix string) error
}

// UploadNotifier is implemented by stores that can signal finalized
// uploads in-process (the dev store). New registers FinalizeUpload with
// it automatically. Stores with out-of-band signals (GCS → Pub/Sub) are
// wired explicitly in the deployment instead — see gblob.Watch.
type UploadNotifier interface {
	NotifyUploads(finalize func(ctx context.Context, key string) error)
}

// UploadDef is the generated wiring for one schema `upload` block.
type UploadDef struct {
	Name  string
	Owner string // enclosing aggregate/record name
	// OnStarted optionally names the command dispatched when a session is
	// created; OnUploaded names the one dispatched when the object
	// finalizes. StartedField/UploadedField are their `file` fields' json
	// names.
	OnStarted     string
	StartedField  string
	OnUploaded    string
	UploadedField string
}

func (r *Registry) uploadDef(name string) *UploadDef {
	for _, u := range r.Uploads {
		if u.Name == name {
			return u
		}
	}
	return nil
}

// UploadRequest asks for a new upload session against a declared upload.
type UploadRequest struct {
	Upload      string    `json:"upload"`
	Namespace   string    `json:"namespace"`
	StreamID    uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	ContentType string    `json:"content_type"`
	Size        int64     `json:"size"`
	Origin      string    `json:"-"`
}

// Upload is a created session: the file identity the domain will see,
// the URL the client uploads chunks to, and the protocol to speak at it.
type Upload struct {
	File     FileRef `json:"file"`
	URL      string  `json:"url"`
	Protocol string  `json:"protocol"`
}

const (
	metaUpload = "loom_upload"
	metaName   = "loom_name"
	metaFileID = "loom_file_id"
)

// uploadKey is the object key layout. The service/namespace/streamID
// prefix is load-bearing: Shred deletes by it.
func (c *Client) uploadKey(namespace string, streamID, fileID uuid.UUID, upload string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", c.reg.Service, namespace, streamID, upload, fileID)
}

func (c *Client) shredPrefix(namespace string, streamID uuid.UUID) string {
	return fmt.Sprintf("%s/%s/%s/", c.reg.Service, namespace, streamID)
}

// CreateUpload opens a resumable session for a schema-declared upload
// and, if the upload declares `on started`, dispatches that command in
// the caller's metadata context. The session URL goes back to the
// client; nothing further happens until storage reports the object
// finalized.
func (c *Client) CreateUpload(ctx context.Context, req UploadRequest) (*Upload, error) {
	def := c.reg.uploadDef(req.Upload)
	if def == nil {
		return nil, fmt.Errorf("loom: unknown upload %q", req.Upload)
	}
	if c.blobs == nil {
		return nil, fmt.Errorf("loom: upload %s: Config.Blobs is not set", req.Upload)
	}
	if req.Namespace == "" || req.StreamID == uuid.Nil {
		return nil, fmt.Errorf("loom: upload %s needs a namespace and a stream id", req.Upload)
	}
	if req.Size <= 0 {
		return nil, fmt.Errorf("loom: upload %s needs a positive size", req.Upload)
	}

	fileID := uuid.New()
	ref := FileRef{
		ID:          fileID,
		Key:         c.uploadKey(req.Namespace, req.StreamID, fileID, def.Name),
		Name:        req.Name,
		ContentType: req.ContentType,
		Size:        req.Size,
	}
	sess, err := c.blobs.CreateUpload(ctx, UploadInit{
		Key:         ref.Key,
		Name:        req.Name,
		ContentType: req.ContentType,
		Size:        req.Size,
		Origin:      req.Origin,
		Metadata: map[string]string{
			metaUpload: def.Name,
			metaName:   req.Name,
			metaFileID: fileID.String(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("loom: upload %s: %w", req.Upload, err)
	}
	if sess.Protocol == "" {
		return nil, fmt.Errorf("loom: upload %s: the blob store returned no protocol — clients must know the chunk dialect", req.Upload)
	}
	if def.OnStarted != "" {
		cmd, err := c.uploadCommand(def.OnStarted, def.StartedField, req.Namespace, req.StreamID, ref)
		if err != nil {
			return nil, err
		}
		if err := c.Dispatch(ctx, cmd); err != nil {
			return nil, fmt.Errorf("loom: upload %s: %s: %w", req.Upload, def.OnStarted, err)
		}
	}
	return &Upload{File: ref, URL: sess.URL, Protocol: sess.Protocol}, nil
}

// FinalizeUpload reacts to storage reporting an object complete: verify
// it exists, rebuild the FileRef from what storage actually holds, and
// dispatch the upload's `on uploaded` command. At-least-once callers are
// expected (Pub/Sub redelivers); dispatches dedup on the object key like
// foreign-event deliveries do.
func (c *Client) FinalizeUpload(ctx context.Context, key string) error {
	upload, namespace, streamID, err := c.parseUploadKey(key)
	if err != nil {
		return err
	}
	def := c.reg.uploadDef(upload)
	if def == nil {
		return fmt.Errorf("loom: finalize %s: unknown upload %q", key, upload)
	}
	info, err := c.blobs.Stat(ctx, key)
	if err != nil {
		return fmt.Errorf("loom: finalize %s: %w", key, err)
	}
	if info == nil {
		return fmt.Errorf("loom: finalize %s: object does not exist", key)
	}

	done, err := c.alreadyProcessed(ctx, "uploads", key)
	if err != nil || done {
		return err
	}

	fileID, _ := uuid.Parse(info.Metadata[metaFileID])
	ref := FileRef{
		ID:          fileID,
		Key:         key,
		Name:        info.Metadata[metaName],
		ContentType: info.ContentType,
		Size:        info.Size,
	}
	cmd, err := c.uploadCommand(def.OnUploaded, def.UploadedField, namespace, streamID, ref)
	if err != nil {
		return err
	}
	ctx = WithMeta(ctx, Metadata{
		CorrelationID: MetaFrom(ctx).CorrelationID,
		CausationID:   "upload:" + key,
		Actor:         MetaFrom(ctx).Actor,
	})
	if err := c.Dispatch(ctx, cmd); err != nil {
		return fmt.Errorf("loom: finalize %s: %s: %w", key, def.OnUploaded, err)
	}
	return c.markProcessed(ctx, "uploads", key)
}

// parseUploadKey inverts uploadKey: service/namespace/streamID/upload/fileID.
func (c *Client) parseUploadKey(key string) (upload, namespace string, streamID uuid.UUID, err error) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[0] != c.reg.Service {
		return "", "", uuid.Nil, fmt.Errorf("%w: %q (service %s)", ErrForeignObject, key, c.reg.Service)
	}
	id, err := uuid.Parse(parts[2])
	if err != nil {
		return "", "", uuid.Nil, fmt.Errorf("%w: %q: bad stream id", ErrForeignObject, key)
	}
	return parts[3], parts[1], id, nil
}

// uploadCommand builds a lifecycle command: target plus the single
// `file` field the schema validated it to have.
func (c *Client) uploadCommand(name, field, namespace string, streamID uuid.UUID, ref FileRef) (Command, error) {
	raw, err := json.Marshal(map[string]any{
		"aggregate_id": streamID,
		"namespace":    namespace,
		field:          ref,
	})
	if err != nil {
		return nil, err
	}
	var def func() Command
	if _, cd := c.reg.aggregateForCommand(name); cd != nil {
		def = cd.New
	} else if _, rd := c.reg.recordForCommand(name); rd != nil {
		def = rd.New
	} else {
		return nil, fmt.Errorf("loom: upload dispatches unknown command %s", name)
	}
	cmd := def()
	return cmd, json.Unmarshal(raw, cmd)
}
