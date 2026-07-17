package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestUploadLifecycle drives the whole upload path on the dev store:
// CreateUpload opens a session and dispatches `on started`; chunked PUTs
// with Content-Range land the object; the synchronous finalize dispatches
// `on uploaded`; the FileRef folds into state; /files streams the bytes
// back; a redelivered finalize converges; Shred deletes the objects.
func TestUploadLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	mux := http.NewServeMux()
	blobSrv := httptest.NewServer(mux)
	defer blobSrv.Close()
	store := loom.NewDirBlobStore(t.TempDir(), blobSrv.URL+"/blobs")
	mux.Handle("/blobs/", http.StripPrefix("/blobs", store))

	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	orderID := uuid.New()
	base := loom.CommandBase{AggregateID: orderID, Namespace: "acme"}
	if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: base,
		CustomerId:  uuid.New(),
		Items:       []ordersgen.OrderItem{{Sku: "sku-1", Quantity: 1, PriceCents: 100}},
		Currency:    "USD",
	}); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("signed! "), 512) // 4096 bytes, two chunks
	up, err := cli.CreateUpload(ctx, loom.UploadRequest{
		Upload:      "Contract",
		Namespace:   "acme",
		StreamID:    orderID,
		Name:        "contract.pdf",
		ContentType: "application/pdf",
		Size:        int64(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if up.File.Key == "" || up.URL == "" || up.Protocol != loom.ProtocolGCSResumable {
		t.Fatalf("bad session: %+v", up)
	}

	// `on started` dispatched RequestContract; the state's contract stays
	// empty until the object actually lands
	state, version, err := cli.Load(ctx, "Order", "acme", orderID)
	if err != nil {
		t.Fatal(err)
	}
	order := state.(*ordersgen.Order)
	if version != 2 || order.Contract != nil {
		t.Fatalf("after started: version=%d contract=%+v", version, order.Contract)
	}

	// two chunks, GCS resumable shape: 308 then 200
	putChunk := func(start, end int) *http.Response {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, up.URL, bytes.NewReader(payload[start:end]))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(payload)))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp
	}
	if resp := putChunk(0, 2048); resp.StatusCode != 308 || resp.Header.Get("Range") != "bytes=0-2047" {
		t.Fatalf("first chunk: status=%d range=%q", resp.StatusCode, resp.Header.Get("Range"))
	}
	if resp := putChunk(2048, len(payload)); resp.StatusCode != http.StatusOK {
		t.Fatalf("final chunk: status=%d", resp.StatusCode)
	}

	// finalize ran synchronously: AttachContract folded
	state, version, err = cli.Load(ctx, "Order", "acme", orderID)
	if err != nil {
		t.Fatal(err)
	}
	order = state.(*ordersgen.Order)
	if version != 3 || order.Contract == nil || order.Contract.Size != int64(len(payload)) {
		t.Fatalf("ContractAttached not folded: version=%d contract=%+v", version, order.Contract)
	}

	// a redelivered finalize signal converges without a new event
	if err := cli.FinalizeUpload(ctx, up.File.Key); err != nil {
		t.Fatal(err)
	}
	if _, v, _ := cli.Load(ctx, "Order", "acme", orderID); v != 3 {
		t.Fatalf("redelivered finalize appended events: version=%d", v)
	}

	// the bytes come back through the gateway's Files handler — the
	// services' own HTTP surfaces can stay private
	files := httptest.NewServer(loomgql.Files(cli))
	defer files.Close()
	resp, err := http.Get(files.URL + "/?key=" + url.QueryEscape(up.File.Key))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, payload) {
		t.Fatalf("gateway files: status=%d len=%d want %d", resp.StatusCode, len(got), len(payload))
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/pdf" {
		t.Fatalf("gateway files content type: %q", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); cd != `attachment; filename="contract.pdf"` {
		t.Fatalf("gateway files disposition: %q", cd)
	}
	// a key for a service the gateway doesn't hold is a 404, not a leak
	if resp, err := http.Get(files.URL + "/?key=other/ns/x/U/f"); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign service key: %v %v", resp.StatusCode, err)
	}

	// the service's own /files endpoint still serves (for internal use)
	api := httptest.NewServer(cli.HTTPHandler())
	defer api.Close()
	resp, err = http.Get(api.URL + "/files?key=" + url.QueryEscape(up.File.Key))
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, payload) {
		t.Fatalf("/files: status=%d len=%d want %d", resp.StatusCode, len(got), len(payload))
	}

	// shred erases the stream's files
	if err := cli.Shred(ctx, "acme", orderID); err != nil {
		t.Fatal(err)
	}
	if info, err := store.Stat(ctx, up.File.Key); err != nil || info != nil {
		t.Fatalf("contract survived shred: %+v err=%v", info, err)
	}
}

// TestUploadAPI drives the same flow through the HTTP surface: POST
// /uploads opens the session, a single whole-body PUT completes it.
func TestUploadAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	store := loom.NewDirBlobStore(t.TempDir(), srv.URL+"/blobs")
	mux.Handle("/blobs/", http.StripPrefix("/blobs", store))

	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	mux.Handle("/loom/", http.StripPrefix("/loom", cli.HTTPHandler()))

	orderID := uuid.New()
	if err := cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: orderID, Namespace: "acme"},
		CustomerId:  uuid.New(),
		Items:       []ordersgen.OrderItem{{Sku: "sku-1", Quantity: 1, PriceCents: 100}},
		Currency:    "USD",
	}); err != nil {
		t.Fatal(err)
	}

	body := fmt.Sprintf(`{"upload":"Contract","namespace":"acme","id":%q,"name":"c.pdf","content_type":"application/pdf","size":9}`, orderID)
	resp, err := http.Post(srv.URL+"/loom/uploads", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	var up loom.Upload
	err = json.NewDecoder(resp.Body).Decode(&up)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /uploads: status=%d err=%v", resp.StatusCode, err)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, up.URL, bytes.NewReader([]byte("signed!!!")))
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, putResp.Body)
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("whole-body PUT: status=%d", putResp.StatusCode)
	}

	state, version, err := cli.Load(ctx, "Order", "acme", orderID)
	if err != nil {
		t.Fatal(err)
	}
	if order := state.(*ordersgen.Order); version != 3 || order.Contract == nil || order.Contract.Size != 9 {
		t.Fatalf("upload not attached: version=%d %+v", version, order.Contract)
	}
}
