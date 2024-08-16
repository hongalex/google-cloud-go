// Copyright 2014 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
)

func ExampleNewClient() {
	ctx := context.Background()
	_, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	// See the other examples to learn how to use the Client.
}

func ExamplePublisher_Publish() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	topic := client.Publisher("topicName")
	defer topic.Stop()
	var results []*pubsub.PublishResult
	r := topic.Publish(ctx, &pubsub.Message{
		Data: []byte("hello world"),
	})
	results = append(results, r)
	// Do other work ...
	for _, r := range results {
		id, err := r.Get(ctx)
		if err != nil {
			// TODO: Handle error.
		}
		fmt.Printf("Published a message with a message ID: %s\n", id)
	}
}

func ExampleSubscription_Receive() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	sub := client.Subscription("subName")
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// TODO: Handle message.
		// NOTE: May be called concurrently; synchronize access to shared memory.
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		// TODO: Handle error.
	}
}

// This example shows how to configure keepalive so that unacknoweldged messages
// expire quickly, allowing other subscribers to take them.
func ExampleSubscription_Receive_maxExtension() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	sub := client.Subscription("subName")
	// This program is expected to process and acknowledge messages in 30 seconds. If
	// not, the Pub/Sub API will assume the message is not acknowledged.
	sub.ReceiveSettings.MaxExtension = 30 * time.Second
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// TODO: Handle message.
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		// TODO: Handle error.
	}
}

// This example shows how to throttle Subscription.Receive, which aims for high
// throughput by default. By limiting the number of messages and/or bytes being
// processed at once, you can bound your program's resource consumption.
func ExampleSubscription_Receive_maxOutstanding() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	sub := client.Subscription("subName")
	sub.ReceiveSettings.MaxOutstandingMessages = 5
	sub.ReceiveSettings.MaxOutstandingBytes = 10e6
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// TODO: Handle message.
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		// TODO: Handle error.
	}
}

func ExampleSubscription_Update() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	sub := client.Subscription("subName")
	subConfig, err := sub.Update(ctx, pubsub.SubscriptionConfigToUpdate{
		PushConfig: &pubsub.PushConfig{Endpoint: "https://example.com/push"},
		// Make the subscription never expire.
		ExpirationPolicy: time.Duration(0),
	})
	if err != nil {
		// TODO: Handle error.
	}
	_ = subConfig // TODO: Use SubscriptionConfig.
}
