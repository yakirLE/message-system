package nats

import (
	"context"
	"fmt"
	"time"

	natssdk "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

//go:generate docker run --name nats --rm -p 4222:4222 -p 8222:8222 nats -js --http_port 8222
type impl struct {
	conn            *natssdk.Conn
	js              jetstream.JetStream
	stream          jetstream.Stream
	consumeContexts []jetstream.ConsumeContext
	cfg             config
	streamName      string
	subjects        []string
}

func New(cfg config, streamName string, subjects ...string) *impl {
	return &impl{
		cfg:        cfg,
		streamName: streamName,
		subjects:   subjects,
	}
}

func (n *impl) Register(ctx context.Context) error {
	nc, err := connectToNATS(n.cfg.GetString("nats.url"), n.cfg.GetInt("nats.port"))
	if err != nil {
		return err
	}

	js, err := createNatsJetStream(nc)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(n.cfg.GetInt("nats.timeout"))*time.Second)
	defer cancel()

	stream, err := createOrUpdateStream(ctx, js, n.streamName, n.subjects)
	if err != nil {
		return fmt.Errorf("register failed: %w", err)
	}

	n.js = js
	n.stream = stream

	return nil
}

func (n *impl) PublishMessage(ctx context.Context, subject string, payload []byte) error {
	if !n.isSubjectSupported(subject) {
		return fmt.Errorf("subject not supported: %s", subject)
	}

	return publishMessage(ctx, n.js, subject, payload)
}

func (n *impl) PollMessages(ctx context.Context, subject string, maxMessages int, autoAck bool) ([]jetstream.Msg, error) {
	if !n.isSubjectSupported(subject) {
		return nil, fmt.Errorf("subject not supported: %s", subject)
	}

	consumer, err := createOrUpdateConsumer(ctx, n.stream, n.streamName, true)
	if err != nil {
		return nil, err
	}

	msgs := fetchMessages(ctx, consumer, maxMessages, autoAck)

	return msgs, nil
}

func (n *impl) ConsumeMessages(ctx context.Context, subject string, maxMessages int, autoAck bool, msgHandler func(jetstream.Msg)) error {
	if !n.isSubjectSupported(subject) {
		return fmt.Errorf("subject not supported: %s", subject)
	}

	consumer, err := createOrUpdateConsumer(ctx, n.stream, n.streamName, true)
	if err != nil {
		return err
	}

	cctx, err := consume(ctx, consumer, maxMessages, autoAck, msgHandler)
	if err != nil {
		return err
	}

	n.consumeContexts = append(n.consumeContexts, cctx)

	return nil
}

func (n *impl) Close() {
	for _, cctx := range n.consumeContexts {
		cctx.Stop()
	}

	n.conn.Close()
}

func (n *impl) isSubjectSupported(subject string) bool {
	for _, s := range n.subjects {
		if subject == s {
			return true
		}
	}

	return false
}

func connectToNATS(url string, port int) (*natssdk.Conn, error) {
	nc, err := natssdk.Connect(fmt.Sprintf("nats://%s:%d", url, port))
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	return nc, nil
}

func createNatsJetStream(nc *natssdk.Conn) (jetstream.JetStream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	return js, nil
}

func createOrUpdateStream(ctx context.Context, js jetstream.JetStream, streamName string, subjects []string) (jetstream.Stream, error) {
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        streamName,
		Description: streamName,
		Subjects:    subjects,
		MaxBytes:    1024 * 1024 * 1024,
	})

	if err != nil {
		return nil, fmt.Errorf("create or update stream: %w", err)
	}

	return stream, nil
}

func publishMessage(ctx context.Context, js jetstream.JetStream, subject string, payload []byte) error {
	_, err := js.Publish(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

func createOrUpdateConsumer(ctx context.Context, stream jetstream.Stream, streamName string, ephemeral bool) (jetstream.Consumer, error) {
	var durable string
	if !ephemeral {
		durable = streamName
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          streamName,
		Durable:       durable,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       5 * time.Second,
		MaxAckPending: -1,
	})

	if err != nil {
		return nil, fmt.Errorf("create or update consumer: %w", err)
	}

	return consumer, nil
}

func consume(_ context.Context, consumer jetstream.Consumer, maxMessages int, autoAck bool, msgHandler func(jetstream.Msg)) (jetstream.ConsumeContext, error) {
	f := msgHandler
	if autoAck {
		f = func(msg jetstream.Msg) {
			msgHandler(msg)
			msg.Ack()
		}
	}

	cctx, err := consumer.Consume(f, jetstream.PullMaxMessages(maxMessages))
	if err != nil {
		return nil, fmt.Errorf("consume error: %w", err)
	}

	return cctx, nil
}

func fetchMessages(_ context.Context, consumer jetstream.Consumer, maxMessages int, autoAck bool) []jetstream.Msg {
	messages := make([]jetstream.Msg, 0, maxMessages)
	iter, _ := consumer.Messages(jetstream.PullMaxMessages(maxMessages))
	for i := 0; i < maxMessages; i++ {
		msg, err := iter.Next()
		if err != nil {
			break
		}

		messages = append(messages, msg)
		if autoAck {
			msg.Ack()
		}
	}

	iter.Stop()

	return messages
}

type config interface {
	GetString(key string) string
	GetInt(key string) int
}
