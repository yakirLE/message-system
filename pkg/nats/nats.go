package nats

import (
	"context"
	"fmt"
	"time"

	natssdk "github.com/nats-io/nats.go"
)

type impl struct {
	conn       *natssdk.Conn
	jsCtx      natssdk.JetStreamContext
	streamName string
	subjects   []string
}

func New(ctx context.Context, cfg config, streamName string, subjects ...string) (*impl, error) {
	nc, err := connectToNATS(cfg.GetString("nats.url"), cfg.GetInt("nats.port"))
	if err != nil {
		return nil, err
	}

	jsCtx, err := createNatsJetStream(nc)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.GetInt("nats.timeout"))*time.Second)
	defer cancel()

	_, err = addStream(ctx, jsCtx, streamName, subjects)
	if err != nil {
		return nil, err
	}

	return &impl{
		conn:       nc,
		jsCtx:      jsCtx,
		streamName: streamName,
		subjects:   subjects,
	}, nil
}

func (n *impl) PublishMessage(subject string, payload []byte) error {
	if !n.isSubjectSupported(subject) {
		return fmt.Errorf("subject not supported: %s", subject)
	}

	return publishMessage(n.conn, subject, payload)
}

func (n *impl) PollMessage(ctx context.Context, subject string) (string, string, []byte, error) {
	if !n.isSubjectSupported(subject) {
		return "", "", nil, fmt.Errorf("subject not supported: %s", subject)
	}

	_, err := createEphemeralConsumer(ctx, n.jsCtx, n.streamName)
	if err != nil {
		return "", "", nil, err
	}

	subscriber, err := subscribe(ctx, n.jsCtx, subject, n.streamName)
	if err != nil {
		return "", "", nil, err
	}

	msg, err := fetchOne(ctx, subscriber)
	if err != nil {
		return "", "", nil, err
	}

	return msg.Subject, msg.Reply, msg.Data, nil
}

func (n *impl) isSubjectSupported(subject string) bool {
	var found bool
	for _, s := range n.subjects {
		if subject == s {
			found = true
			break
		}
	}

	return found
}

func connectToNATS(url string, port int) (*natssdk.Conn, error) {
	nc, err := natssdk.Connect(fmt.Sprintf("nats://%s:%d", url, port))
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	return nc, nil
}

func createNatsJetStream(nc *natssdk.Conn) (natssdk.JetStreamContext, error) {
	jsCtx, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	return jsCtx, nil
}

func addStream(ctx context.Context, jsCtx natssdk.JetStreamContext, streamName string, subjects []string) (*natssdk.StreamInfo, error) {
	stream, err := jsCtx.AddStream(&natssdk.StreamConfig{
		Name:              streamName,
		Subjects:          subjects,
		Retention:         natssdk.InterestPolicy,
		Discard:           natssdk.DiscardOld,
		MaxAge:            7 * 24 * time.Hour,
		Storage:           natssdk.FileStorage,
		MaxMsgsPerSubject: 100_000_000,
		MaxMsgSize:        4 << 20,
		NoAck:             false,
	}, natssdk.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("add stream: %w", err)
	}

	return stream, nil
}

func publishMessage(nc *natssdk.Conn, subject string, payload []byte) error {
	err := nc.Publish(subject, payload)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

func createEphemeralConsumer(ctx context.Context, jsCtx natssdk.JetStreamContext, streamName string) (*natssdk.ConsumerInfo, error) {
	consumer, err := jsCtx.AddConsumer(streamName, &natssdk.ConsumerConfig{
		// Durable:       consumerGroupName,
		DeliverPolicy: natssdk.DeliverAllPolicy,
		AckPolicy:     natssdk.AckExplicitPolicy,
		AckWait:       5 * time.Second,
		MaxAckPending: -1,
	}, natssdk.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("add consumer: %w", err)
	}

	return consumer, nil
}

func subscribe(ctx context.Context, jsCtx natssdk.JetStreamContext, subject, streamName string) (*natssdk.Subscription, error) {
	pullSub, err := jsCtx.PullSubscribe(
		subject,
		"",
		natssdk.ManualAck(),
		// natssdk.Bind(streamName, consumerGroupName),
		natssdk.Context(ctx),
	)

	if err != nil {
		return nil, fmt.Errorf("pull subscribe: %w", err)
	}

	return pullSub, nil
}

func fetchOne(ctx context.Context, pullSub *natssdk.Subscription) (*natssdk.Msg, error) {
	msgs, err := pullSub.Fetch(1, natssdk.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}

	if len(msgs) == 0 {
		return nil, fmt.Errorf("no messages")
	}

	return msgs[0], nil
}

type config interface {
	GetString(key string) string
	GetInt(key string) int
}
