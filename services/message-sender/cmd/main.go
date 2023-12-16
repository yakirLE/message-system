package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

//go:generate docker run --name nats --rm -p 4222:4222 -p 8222:8222 nats -js --http_port 8222
func main() {
	ctx := context.TODO()
	fmt.Println("connecting...")
	conn, err := connectToNATS()
	handleError(err)
	fmt.Println("getting JetStream context...")
	jsCtx, err := natsJetStream(conn)
	handleError(err)
	fmt.Println("creating stream...")
	_, err = createStream(ctx, jsCtx)
	handleError(err)
	fmt.Println("publishing message...")
	err = publishMessage(conn, "subject.1", []byte("this is my payload"))
	handleError(err)

	fmt.Println("creating consumer...")
	_, err = createConsumer(ctx, jsCtx, "my_group", "test_stream")
	handleError(err)
	fmt.Println("subscribing...")
	sub, err := subscribe(ctx, jsCtx, "subject.1", "my_group", "test_stream")
	handleError(err)
	fmt.Println("fetching message...")
	msg, err := fetchOne(ctx, sub)
	handleError(err)
	fmt.Println("marshaling message...")
	b, _ := json.Marshal(msg)
	fmt.Println("message:", string(b))
	fmt.Println("message data:", string(msg.Data))
	fmt.Println("done")
}

func handleError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func connectToNATS() (*nats.Conn, error) {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	return nc, nil
}

func natsJetStream(nc *nats.Conn) (nats.JetStreamContext, error) {
	jsCtx, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	return jsCtx, nil
}

func createStream(ctx context.Context, jsCtx nats.JetStreamContext) (*nats.StreamInfo, error) {
	stream, err := jsCtx.AddStream(&nats.StreamConfig{
		Name:              "test_stream",
		Subjects:          []string{"subject.1", "subject.2", "subject.N"},
		Retention:         nats.InterestPolicy,
		Discard:           nats.DiscardOld,
		MaxAge:            7 * 24 * time.Hour,
		Storage:           nats.FileStorage,
		MaxMsgsPerSubject: 100_000_000,
		MaxMsgSize:        4 << 20,
		NoAck:             false,
	}, nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("add stream: %w", err)
	}

	return stream, nil
}

func publishMessage(nc *nats.Conn, subject string, payload []byte) error {
	err := nc.Publish(subject, payload)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

func createConsumer(ctx context.Context, jsCtx nats.JetStreamContext, consumerGroupName, streamName string) (*nats.ConsumerInfo, error) {
	consumer, err := jsCtx.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerGroupName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       5 * time.Second,
		MaxAckPending: -1,
	}, nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("add consumer: %w", err)
	}

	return consumer, nil
}

func subscribe(ctx context.Context, jsCtx nats.JetStreamContext, subject, consumerGroupName, streamName string) (*nats.Subscription, error) {
	pullSub, err := jsCtx.PullSubscribe(
		subject,
		consumerGroupName,
		nats.ManualAck(),
		nats.Bind(streamName, consumerGroupName),
		nats.Context(ctx),
	)

	if err != nil {
		return nil, fmt.Errorf("pull subscribe: %w", err)
	}

	return pullSub, nil
}

func fetchOne(ctx context.Context, pullSub *nats.Subscription) (*nats.Msg, error) {
	msgs, err := pullSub.Fetch(1, nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}

	if len(msgs) == 0 {
		return nil, fmt.Errorf("no messages")
	}

	return msgs[0], nil
}
