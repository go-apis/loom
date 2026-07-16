// Package gpub is the Google Cloud Pub/Sub transport for loom: one shared
// topic, one durable subscription per consumer group (service.process).
// Handler errors nack for broker redelivery; the loom process runner owns
// retries, dedup, and parking, so nothing is ever dropped at the transport.
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
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
}

type Bus struct {
	client *pubsub.Client
	topic  *pubsub.Topic
	codec  Codec
	log    *slog.Logger

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
	topic, err := ensureTopic(ctx, client, cfg.TopicID)
	if err != nil {
		client.Close()
		return nil, err
	}
	cctx, cancel := context.WithCancel(context.Background())
	return &Bus{
		client: client,
		topic:  topic,
		codec:  cfg.Codec,
		log:    cfg.Logger,
		cctx:   cctx,
		cancel: cancel,
	}, nil
}

func emulated() bool { return os.Getenv("PUBSUB_EMULATOR_HOST") != "" }

func ensureTopic(ctx context.Context, client *pubsub.Client, id string) (*pubsub.Topic, error) {
	topic := client.Topic(id)
	ok, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		if _, err := client.CreateTopic(ctx, id); err != nil && status.Code(err) != codes.AlreadyExists {
			return nil, err
		}
	}
	topic.EnableMessageOrdering = !emulated()
	topic.PublishSettings.ByteThreshold = 5000
	topic.PublishSettings.CountThreshold = 10
	topic.PublishSettings.DelayThreshold = 100 * time.Millisecond
	return topic, nil
}

func (b *Bus) Publish(ctx context.Context, env *loom.Envelope) error {
	data, err := b.codec.Marshal(env)
	if err != nil {
		return err
	}
	key := env.OrderingKey()
	if !b.topic.EnableMessageOrdering {
		key = ""
	}
	res := b.topic.Publish(ctx, &pubsub.Message{
		Data:        data,
		OrderingKey: key,
		Attributes: map[string]string{
			"service":   env.Service,
			"type":      env.Type,
			"namespace": env.Namespace,
		},
	})
	if _, err := res.Get(ctx); err != nil {
		// a failed publish pauses its ordering key on this topic handle;
		// without a resume every retry of the same key fails for the life
		// of the process
		b.topic.ResumePublish(key)
		return err
	}
	return nil
}

// Subscribe attaches a durable consumer group: subscription id
// "<topic>__<group>", created on demand. The handler runs per delivery;
// an error nacks for redelivery.
func (b *Bus) Subscribe(ctx context.Context, group string, handler func(ctx context.Context, env *loom.Envelope) error) error {
	sub, err := b.ensureSubscription(ctx, group)
	if err != nil {
		return err
	}
	b.wg.Add(1)
	go b.receiveLoop(sub, group, handler)
	return nil
}

func (b *Bus) ensureSubscription(ctx context.Context, group string) (*pubsub.Subscription, error) {
	id := b.topic.ID() + "__" + strings.ReplaceAll(group, ".", "-")
	sub := b.client.Subscription(id)
	ok, err := sub.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		_, err = b.client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{
			Topic:                 b.topic,
			AckDeadline:           10 * time.Second,
			EnableMessageOrdering: !emulated(),
			RetryPolicy:           &pubsub.RetryPolicy{MinimumBackoff: 10 * time.Millisecond},
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return nil, err
		}
		sub = b.client.Subscription(id)
	}
	sub.ReceiveSettings.MaxOutstandingMessages = 100
	return sub, nil
}

func (b *Bus) receiveLoop(sub *pubsub.Subscription, group string, handler func(ctx context.Context, env *loom.Envelope) error) {
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
	b.topic.Stop()
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
