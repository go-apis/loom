package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// An instance's generation orders its build against the service's other
// instances for the process lease (election.go, ADR 0010). It must grow
// with every deploy, and it must be the same on every instance of one
// revision. In order, it is:
//
//  1. Config.Generation, when positive: the service computes it.
//  2. LOOM_GENERATION, when set: the deploy sets it, by convention to its
//     Unix time in seconds (gcloud run deploy --update-env-vars
//     LOOM_GENERATION=$(date +%s)).
//  3. On Cloud Run (K_SERVICE and K_REVISION set), the revision's
//     createTime in Unix seconds, read from the Cloud Run Admin API with
//     the metadata server's token. The service account needs
//     run.revisions.get on the service (roles/run.viewer). It is asked
//     once, when the election starts.
//  4. None: the election is as it was before ADR 0010. The instance
//     never takes the lease from anyone, and a leader with none is never
//     taken from.
//
// A negative Config.Generation or LOOM_GENERATION turns generations off.
// LOOM_GENERATION and a revision's createTime are both Unix seconds, so a
// service that starts or stops setting the variable still compares a
// deploy with the one before it.
//
// What a generation is not: the sequence in a Cloud Run revision's name
// (latchkey-00380-xih). loom v0.85.0 used it, and Cloud Run does not
// number revisions in deploy order: latchkey-00355-7rg was deployed
// hours after latchkey-00380-xih, and the older build kept the lease.

// generation is this instance's build as the process election orders it.
type generation struct {
	n        int64  // 0: none — the election ignores builds
	revision string // a name for logs and the lease session; may be empty
	source   string // where n came from, for logs
	lookup   bool   // n is to be read from Cloud Run when the election starts
	note     string // why there is none, logged when the election starts
}

const (
	genFromConfig    = "Config.Generation"
	genFromEnv       = "LOOM_GENERATION"
	genFromCloudRun  = "cloud run revision createTime"
	generationEnvVar = "LOOM_GENERATION"
)

// generationFrom resolves Config.Generation, LOOM_GENERATION and
// Config.Revision (default K_REVISION). A Cloud Run lookup it leaves to
// resolveGeneration, off the constructor's path.
func generationFrom(n int64, revision string) generation {
	if revision == "" {
		revision = os.Getenv("K_REVISION")
	}
	g := generation{revision: revision}
	switch {
	case n < 0:
		g.note = "turned off by Config.Generation"
		return g
	case n > 0:
		g.n, g.source = n, genFromConfig
		return g
	}
	if v := strings.TrimSpace(os.Getenv(generationEnvVar)); v != "" {
		env, err := strconv.ParseInt(v, 10, 64)
		switch {
		case err != nil || env == 0:
			// not a number: no generation rather than a wrong one
			g.note = fmt.Sprintf("%s=%q is not a generation", generationEnvVar, v)
		case env < 0:
			g.note = "turned off by " + generationEnvVar
		default:
			g.n, g.source = env, genFromEnv
		}
		return g
	}
	if os.Getenv("K_SERVICE") != "" && revision != "" {
		g.lookup = true
		return g
	}
	g.note = "neither Config.Generation nor " + generationEnvVar + " is set, and this is not a Cloud Run service"
	return g
}

// resolveGeneration finishes generationFrom when the election starts: it
// asks Cloud Run for the revision's createTime if that is the source, and
// logs the generation, or why there is none. Any failure is no
// generation, never a guess.
func (c *Client) resolveGeneration(ctx context.Context) {
	if c.gen.lookup {
		look, cancel := context.WithTimeout(ctx, cloudRunLookupTimeout)
		n, err := cloudRunRevisionCreated(look, os.Getenv("K_SERVICE"), c.gen.revision)
		cancel()
		c.gen.lookup = false
		if err != nil {
			c.gen.note = "LOOM_GENERATION is not set and the Cloud Run revision's createTime could not be read: " + err.Error()
		} else {
			c.gen.n, c.gen.source = n, genFromCloudRun
		}
	}
	if c.gen.n > 0 {
		c.log.InfoContext(ctx, "process lease generation", c.genAttrs()...)
		return
	}
	c.log.InfoContext(ctx, "process lease: no generation, a deploy does not move the lease", "reason", c.gen.note, "revision", c.gen.revision)
}

// RevisionSequence is the number in a revision name Cloud Run gives it:
// mesh-api-00021-j42 is 21. ok is false for a name not of that shape (a
// custom --revision-suffix, a local run).
//
// Deprecated: loom v0.85.0 ordered builds by it, but Cloud Run does not
// number a service's revisions in deploy order (latchkey-00355-7rg was
// deployed after latchkey-00380-xih), so it is not a generation. See
// Config.Generation.
func RevisionSequence(revision string) (n int64, ok bool) {
	parts := strings.Split(revision, "-")
	if len(parts) < 3 || parts[len(parts)-1] == "" {
		return 0, false
	}
	seq := parts[len(parts)-2]
	for _, r := range seq {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(seq, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// The Cloud Run lookup's endpoints; tests point them at a fake.
var (
	metadataBase = func() string {
		if h := os.Getenv("GCE_METADATA_HOST"); h != "" {
			return "http://" + h + "/computeMetadata/v1"
		}
		return "http://metadata.google.internal/computeMetadata/v1"
	}
	cloudRunAPI = "https://run.googleapis.com"
)

// cloudRunLookupTimeout bounds the whole lookup; the election waits for it
// before its first try for the lease.
var cloudRunLookupTimeout = 10 * time.Second

// cloudRunRevisionCreated reads revision's createTime, in Unix seconds,
// from the Cloud Run Admin API, as the instance's service account.
func cloudRunRevisionCreated(ctx context.Context, service, revision string) (int64, error) {
	hc := &http.Client{Timeout: cloudRunLookupTimeout}
	md := metadataBase()
	// "projects/<number>/regions/<region>"
	region, err := httpGet(ctx, hc, md+"/instance/region", map[string]string{"Metadata-Flavor": "Google"})
	if err != nil {
		return 0, fmt.Errorf("metadata region: %w", err)
	}
	p := strings.Split(strings.TrimSpace(string(region)), "/")
	if len(p) != 4 || p[0] != "projects" || p[2] != "regions" || p[1] == "" || p[3] == "" {
		return 0, fmt.Errorf("metadata region: unexpected %q", region)
	}
	project, location := p[1], p[3]
	tok, err := httpGet(ctx, hc, md+"/instance/service-accounts/default/token", map[string]string{"Metadata-Flavor": "Google"})
	if err != nil {
		return 0, fmt.Errorf("metadata token: %w", err)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(tok, &token); err != nil || token.AccessToken == "" {
		return 0, errors.New("metadata token: no access_token")
	}
	u := cloudRunAPI + "/v2/projects/" + url.PathEscape(project) + "/locations/" + url.PathEscape(location) +
		"/services/" + url.PathEscape(service) + "/revisions/" + url.PathEscape(revision)
	body, err := httpGet(ctx, hc, u, map[string]string{"Authorization": "Bearer " + token.AccessToken})
	if err != nil {
		return 0, fmt.Errorf("cloud run revision %s (needs run.revisions.get): %w", revision, err)
	}
	var rev struct {
		CreateTime time.Time `json:"createTime"`
	}
	if err := json.Unmarshal(body, &rev); err != nil {
		return 0, fmt.Errorf("cloud run revision %s: %w", revision, err)
	}
	if n := rev.CreateTime.Unix(); !rev.CreateTime.IsZero() && n > 0 {
		return n, nil
	}
	return 0, fmt.Errorf("cloud run revision %s: no createTime", revision)
}

func httpGet(ctx context.Context, hc *http.Client, u string, header map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return body, nil
}
