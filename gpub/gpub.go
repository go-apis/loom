// Package gpub is the Google Cloud Pub/Sub transport for loom: one shared
// topic, one durable subscription per consumer group (service.process).
// Handler errors nack for broker redelivery; the loom process runner owns
// retries, dedup, and parking, so nothing is ever dropped at the transport.
//
// Delivery is streaming pull by default. Set PushEndpoint for PUSH mode —
// Pub/Sub POSTs each message to this service over HTTP (mount PushHandler)
// — which is what scale-to-zero deployments need: a pull subscriber only
// runs while an instance happens to be alive, so cross-service reactions
// stall until traffic wakes the consumer; push deliveries ARE the traffic.
//
// Under PUBSUB_EMULATOR_HOST message ordering is disabled end-to-end: the
// emulator's ordered-message backlog is broken (keyed messages become
// undeliverable once they accumulate) — a lesson inherited from the old
// eventsourcing provider.
package gpub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/go-apis/loom"
)

// Codec translates envelopes to and from the wire. The default is loom's
// native JSON envelope; a legacy codec can bridge another format (e.g. the
// old eventsourcing event shape) during a migration.
type Codec interface {
	Marshal(env *loom.Envelope) ([]byte, error)
	Unmarshal(data []byte) (*loom.Envelope, error)
}

type Config struct {
	ProjectID string
	// TopicID names the shared events topic. Default "loom-events". The
	// topic and subscriptions are created on demand.
	TopicID string
	Codec   Codec
	Logger  *slog.Logger

	// PushEndpoint switches every Subscribe to PUSH delivery: the public
	// HTTPS URL where THIS service mounts PushHandler (e.g.
	// https://svc-xyz.a.run.app/bus/push). Subscriptions are created — or
	// converted from pull — with this endpoint plus a ?group= marker.
	// Empty keeps streaming pull.
	PushEndpoint string
	// PushServiceAccount is the service account email Pub/Sub mints OIDC
	// tokens as on each push (authenticated push). When set, PushHandler
	// verifies the bearer against PushAudience; when empty no token is
	// attached or verified — the emulator and private-network dev only.
	PushServiceAccount string
	// PushAudience is the expected OIDC audience. Defaults to
	// PushEndpoint.
	PushAudience string
}

type Bus struct {
	client    *pubsub.Client
	publisher *pubsub.Publisher
	topicID   string
	codec     Codec
	log       *slog.Logger

	pushEndpoint string
	pushSA       string
	pushAudience string
	mu           sync.RWMutex
	pushHandlers map[string]func(ctx context.Context, env *loom.Envelope) error

	cancel context.CancelFunc
	cctx   context.Context
	wg     sync.WaitGroup
}

// New connects, ensures the topic, and returns a loom.Bus.
func New(ctx context.Context, cfg Config) (*Bus, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("gpub: ProjectID is required")
	}
	if cfg.TopicID == "" {
		cfg.TopicID = "loom-events"
	}
	if cfg.Codec == nil {
		cfg.Codec = nativeCodec{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, err
	}
	if err := ensureTopic(ctx, client, cfg.TopicID); err != nil {
		client.Close()
		return nil, err
	}
	publisher := client.Publisher(cfg.TopicID)
	publisher.EnableMessageOrdering = !emulated()
	publisher.PublishSettings.ByteThreshold = 5000
	publisher.PublishSettings.CountThreshold = 10
	publisher.PublishSettings.DelayThreshold = 100 * time.Millisecond

	if cfg.PushAudience == "" {
		cfg.PushAudience = cfg.PushEndpoint
	}
	cctx, cancel := context.WithCancel(context.Background())
	return &Bus{
		client:       client,
		publisher:    publisher,
		topicID:      cfg.TopicID,
		codec:        cfg.Codec,
		log:          cfg.Logger,
		pushEndpoint: cfg.PushEndpoint,
		pushSA:       cfg.PushServiceAccount,
		pushAudience: cfg.PushAudience,
		pushHandlers: map[string]func(ctx context.Context, env *loom.Envelope) error{},
		cctx:         cctx,
		cancel:       cancel,
	}, nil
}

func emulated() bool { return os.Getenv("PUBSUB_EMULATOR_HOST") != "" }

func (b *Bus) topicName() string {
	return fmt.Sprintf("projects/%s/topics/%s", b.client.Project(), b.topicID)
}

func ensureTopic(ctx context.Context, client *pubsub.Client, id string) error {
	name := fmt.Sprintf("projects/%s/topics/%s", client.Project(), id)
	_, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return err
	}
	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return err
	}
	return nil
}

func (b *Bus) Publish(ctx context.Context, env *loom.Envelope) error {
	data, err := b.codec.Marshal(env)
	if err != nil {
		return err
	}
	key := env.OrderingKey()
	if !b.publisher.EnableMessageOrdering {
		key = ""
	}
	res := b.publisher.Publish(ctx, &pubsub.Message{
		Data:        data,
		OrderingKey: key,
		Attributes: map[string]string{
			"service":   env.Service,
			"type":      env.Type,
			"namespace": env.Namespace,
		},
	})
	if _, err := res.Get(ctx); err != nil {
		// a failed publish pauses its ordering key on this publisher;
		// without a resume every retry of the same key fails for the life
		// of the process
		b.publisher.ResumePublish(key)
		return err
	}
	return nil
}

// Subscribe attaches a durable consumer group: subscription id
// "<topic>__<group>", created on demand. The handler runs per delivery;
// an error nacks for redelivery. In push mode (PushEndpoint set) the
// subscription is created or converted to push and the handler is served
// by PushHandler instead of a receive loop.
func (b *Bus) Subscribe(ctx context.Context, group string, handler func(ctx context.Context, env *loom.Envelope) error) error {
	if b.pushEndpoint != "" {
		if err := b.ensurePushSubscription(ctx, group); err != nil {
			return err
		}
		b.mu.Lock()
		b.pushHandlers[b.subID(group)] = handler
		b.mu.Unlock()
		return nil
	}
	sub, err := b.ensureSubscription(ctx, group)
	if err != nil {
		return err
	}
	b.wg.Add(1)
	go b.receiveLoop(sub, group, handler)
	return nil
}

func (b *Bus) subID(group string) string {
	return b.topicID + "__" + strings.ReplaceAll(group, ".", "-")
}

// pushConfig is the subscription's push target: the service's handler URL
// with the group marker, and OIDC identity when configured.
func (b *Bus) pushConfig(group string) *pubsubpb.PushConfig {
	sep := "?"
	if strings.Contains(b.pushEndpoint, "?") {
		sep = "&"
	}
	pc := &pubsubpb.PushConfig{PushEndpoint: b.pushEndpoint + sep + "group=" + b.subID(group)}
	if b.pushSA != "" {
		pc.AuthenticationMethod = &pubsubpb.PushConfig_OidcToken_{OidcToken: &pubsubpb.PushConfig_OidcToken{
			ServiceAccountEmail: b.pushSA,
			Audience:            b.pushAudience,
		}}
	}
	return pc
}

// ensurePushSubscription creates the subscription in push mode, or
// converts an existing one (a pull deployment migrating to push) by
// updating its push config in place.
func (b *Bus) ensurePushSubscription(ctx context.Context, group string) error {
	id := b.subID(group)
	name := fmt.Sprintf("projects/%s/subscriptions/%s", b.client.Project(), id)
	want := b.pushConfig(group)
	existing, err := b.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return err
		}
		_, err = b.client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:                  name,
			Topic:                 b.topicName(),
			AckDeadlineSeconds:    10,
			EnableMessageOrdering: !emulated(),
			RetryPolicy:           &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(10 * time.Millisecond)},
			PushConfig:            want,
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return err
		}
		return nil
	}
	if existing.PushConfig != nil && existing.PushConfig.PushEndpoint == want.PushEndpoint {
		return nil
	}
	_, err = b.client.SubscriptionAdminClient.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{Name: name, PushConfig: want},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"push_config"}},
	})
	return err
}

// pushMessage is Pub/Sub's push wrapper: the message rides base64-encoded
// with its attributes, alongside the delivering subscription's full name.
type pushMessage struct {
	Message struct {
		Data      []byte `json:"data"` // encoding/json handles the base64
		MessageID string `json:"messageId"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// PushHandler serves push deliveries: mount it at the path PushEndpoint
// points to. 2xx acks; anything else nacks for broker redelivery — loom's
// consumer dedup absorbs the retries exactly as with pull. Undecodable
// payloads ack with a loud log (redelivering garbage helps no one),
// matching the pull path.
func (b *Bus) PushHandler() http.Handler {
	var validate func(ctx context.Context, token string) error
	if b.pushSA != "" {
		validate = func(ctx context.Context, token string) error {
			_, err := idtoken.Validate(ctx, token, b.pushAudience)
			return err
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if validate != nil {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" {
				http.Error(w, "missing bearer", http.StatusUnauthorized)
				return
			}
			if err := validate(ctx, token); err != nil {
				b.log.WarnContext(ctx, "gpub: push token rejected", "error", err)
				http.Error(w, "bad token", http.StatusUnauthorized)
				return
			}
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
		if err != nil {
			http.Error(w, "read", http.StatusBadRequest)
			return
		}
		var pm pushMessage
		if err := json.Unmarshal(body, &pm); err != nil {
			b.log.ErrorContext(ctx, "gpub: dropping undecodable push wrapper", "error", err)
			w.WriteHeader(http.StatusNoContent) // ack: garbage never improves
			return
		}
		// the ?group= marker set at subscription creation is authoritative;
		// the wrapper's subscription name is the fallback (both carry the
		// subscription id, whose group..id mapping is not invertible)
		id := r.URL.Query().Get("group")
		if id == "" {
			if i := strings.LastIndex(pm.Subscription, "/"); i >= 0 {
				id = pm.Subscription[i+1:]
			}
		}
		b.mu.RLock()
		handler := b.pushHandlers[id]
		b.mu.RUnlock()
		if handler == nil {
			// a subscription this instance has not (yet) registered — nack so
			// the broker redelivers once Subscribe has run (boot races)
			http.Error(w, "unknown group "+id, http.StatusServiceUnavailable)
			return
		}
		env, err := b.codec.Unmarshal(pm.Message.Data)
		if err != nil {
			b.log.ErrorContext(ctx, "gpub: dropping undecodable message", "group", id, "error", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err := handler(ctx, env); err != nil {
			b.log.WarnContext(ctx, "gpub: push handler nack", "group", id, "type", env.Type, "error", err)
			http.Error(w, "handler", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func (b *Bus) ensureSubscription(ctx context.Context, group string) (*pubsub.Subscriber, error) {
	id := b.topicID + "__" + strings.ReplaceAll(group, ".", "-")
	name := fmt.Sprintf("projects/%s/subscriptions/%s", b.client.Project(), id)
	_, err := b.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return nil, err
		}
		_, err = b.client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:                  name,
			Topic:                 b.topicName(),
			AckDeadlineSeconds:    10,
			EnableMessageOrdering: !emulated(),
			RetryPolicy:           &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(10 * time.Millisecond)},
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return nil, err
		}
	}
	sub := b.client.Subscriber(id)
	sub.ReceiveSettings.MaxOutstandingMessages = 100
	return sub, nil
}

func (b *Bus) receiveLoop(sub *pubsub.Subscriber, group string, handler func(ctx context.Context, env *loom.Envelope) error) {
	defer b.wg.Done()
	h := func(ctx context.Context, msg *pubsub.Message) {
		env, err := b.codec.Unmarshal(msg.Data)
		if err != nil {
			// undecodable at the transport: ack and log loudly — nacking
			// would redeliver garbage forever
			b.log.ErrorContext(ctx, "gpub: dropping undecodable message", "group", group, "error", err)
			msg.Ack()
			return
		}
		if err := handler(ctx, env); err != nil {
			b.log.WarnContext(ctx, "gpub: handler nack", "group", group, "type", env.Type, "error", err)
			msg.Nack()
			return
		}
		msg.Ack()
	}
	for b.cctx.Err() == nil {
		if err := sub.Receive(b.cctx, h); err != nil && b.cctx.Err() == nil {
			b.log.ErrorContext(b.cctx, "gpub: receive loop retrying", "group", group, "error", err)
			select {
			case <-b.cctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

// Close stops publishing, cancels receive loops, and closes the client.
func (b *Bus) Close() error {
	b.publisher.Stop()
	b.cancel()
	b.wg.Wait()
	return b.client.Close()
}

type nativeCodec struct{}

func (nativeCodec) Marshal(env *loom.Envelope) ([]byte, error) { return json.Marshal(env) }
func (nativeCodec) Unmarshal(data []byte) (*loom.Envelope, error) {
	var env loom.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	if env.Type == "" {
		return nil, errors.New("gpub: envelope has no type")
	}
	return &env, nil
}
