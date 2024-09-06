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

	"cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ExampleNewClient() {
	ctx := context.Background()
	_, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	// See the other examples to learn how to use the Client.
}

func ExampleClient_CreateTopic() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	// Create a new topic with the given name.
	topicPath := fmt.Sprintf("projects/%s/topics/%s", "project-id", "topic-id")
	pbTopic, err := client.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: topicPath})
	if err != nil {
		// TODO: Handle error.
	}
	_ = pbTopic // TODO: use the protobuf topic.
}

func ExampleClient_CreateTopicWithConfig() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	// Create a new topic with the given config.
	t := &pb.Topic{
		Name:       fmt.Sprintf("projects/%s/topics/%s", "project-id", "topic-id"),
		KmsKeyName: "projects/project-id/locations/global/keyRings/my-key-ring/cryptoKeys/my-key",
		MessageStoragePolicy: &pb.MessageStoragePolicy{
			AllowedPersistenceRegions: []string{"us-east1"},
		},
	}
	pbTopic, err := client.TopicAdminClient.CreateTopic(ctx, t)
	if err != nil {
		// TODO: Handle error.
	}
	_ = pbTopic // TODO: use the protobuf topic.
}

// Use Publisher to refer to a topic that is not in the client's project, such
// as a public topic.
func ExampleClient_Publisher() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	otherProjectID := "another-project-id"
	publisher := client.Publisher(fmt.Sprintf("projects/%s/topics/%s", otherProjectID, "my-topic"))
	_ = publisher // TODO: use the publisher client.
}

func ExampleClient_CreateSubscription() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	// Create a new topic with the given name.
	topicName := "projects/project-id/topics/topic-id"
	_, err = client.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: topicName})
	if err != nil {
		// TODO: Handle error.
	}

	// Create a new subscription to the previously created topic
	// with the given name.
	subName := "projects/project-id/subscriptions/subscription-id"
	pbSub, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
		Name:               subName,
		Topic:              topicName,
		AckDeadlineSeconds: 10,
		ExpirationPolicy:   &pb.ExpirationPolicy{Ttl: durationpb.New(25 * time.Hour)},
	})
	if err != nil {
		// TODO: Handle error.
	}

	_ = pbSub // TODO: use the protobuf subscription.
}

func ExampleClient_CreateSubscription_neverExpire() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	// Create a new topic with the given name.
	topicName := "projects/project-id/topics/topic-id"
	_, err = client.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: topicName})
	if err != nil {
		// TODO: Handle error.
	}

	// Create a new subscription to the previously created topic
	// with the given name.
	subName := "projects/project-id/subscriptions/subscription-id"
	pbSub, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
		Name:               subName,
		Topic:              topicName,
		AckDeadlineSeconds: 10,
		ExpirationPolicy:   &pb.ExpirationPolicy{Ttl: durationpb.New(0)},
	})
	if err != nil {
		// TODO: Handle error.
	}
	_ = pbSub // TODO: Use the protobuf subscription
}

func ExampleTopic_Delete() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	topicName := "projects/project-id/topics/topic-id"
	if err := client.TopicAdminClient.DeleteTopic(ctx, &pb.DeleteTopicRequest{Topic: topicName}); err != nil {
		// TODO: Handle error.
	}
}

func ExampleTopic_Publish() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	publisher := client.Publisher("topicName")
	defer publisher.Stop()
	var results []*pubsub.PublishResult
	r := publisher.Publish(ctx, &pubsub.Message{
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

func ExampleTopic_Subscriptions() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	// List all subscriptions of the topic (maybe of multiple projects).
	subs := client.TopicAdminClient.ListTopicSubscriptions(ctx, &pb.ListTopicSubscriptionsRequest{
		Topic: "projects/project-id/topics/topic-id",
	})
	for {
		subName, err := subs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		_ = subName // TODO: Use the subscription string.
	}
}

func ExampleTopic_Update() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	topicName := "projects/project-id/topics/topic-id"
	pbTopic, err := client.TopicAdminClient.UpdateTopic(ctx,
		&pb.UpdateTopicRequest{
			Topic: &pb.Topic{
				Name: topicName,
				MessageStoragePolicy: &pb.MessageStoragePolicy{
					AllowedPersistenceRegions: []string{
						"asia-east1", "asia-northeast1", "asia-southeast1", "australia-southeast1",
						"europe-north1", "europe-west1", "europe-west2", "europe-west3", "europe-west4",
						"us-central1", "us-central2", "us-east1", "us-east4", "us-west1", "us-west2"},
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"message_storage_policy"},
			},
		},
	)

	if err != nil {
		// TODO: Handle error.
	}
	_ = pbTopic // TODO: Use protobuf topic
}

func ExampleTopic_Update_resetMessageStoragePolicy() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	topicName := "projects/project-id/topics/topic-id"
	pbTopic, err := client.TopicAdminClient.UpdateTopic(ctx,
		&pb.UpdateTopicRequest{
			Topic: &pb.Topic{
				Name: topicName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"message_storage_policy"},
			},
		},
	)

	if err != nil {
		// TODO: Handle error.
	}
	_ = pbTopic // TODO: Use protobuf topic
}

func ExampleSubscription_Delete() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	subName := "projects/project-id/subscriptions/sub-id"
	req := &pb.DeleteSubscriptionRequest{Subscription: subName}
	err = client.SubscriptionAdminClient.DeleteSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSubscription_Config() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	subName := "projects/project-id/subscriptions/sub-id"
	req := &pb.GetSubscriptionRequest{Subscription: subName}
	pbSub, err := client.SubscriptionAdminClient.GetSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	fmt.Println(pbSub)
}

func ExampleSubscription_Receive() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	// Can use either "projects/project-id/subscriptions/sub-id" or just "sub-id" here
	sub := client.Subscriber("sub-id")
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// TODO: Handle message.
		// NOTE: May be called concurrently; synchronize access to shared memory.
		m.Ack()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		// TODO: Handle error.
	}
}

// This example shows how to configure keepalive so that unacknowledged messages
// expire quickly, allowing other subscribers to take them.
func ExampleSubscription_Receive_maxExtension() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	sub := client.Subscriber("subNameOrID")
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
	sub := client.Subscriber("subNameOrID")
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
	sub := "projects/project-id/subscriptions/subscription-id"
	req := &pb.UpdateSubscriptionRequest{
		Subscription: &pb.Subscription{
			Name: sub,
			PushConfig: &pb.PushConfig{
				PushEndpoint: "https://example.com/push",
			},
			ExpirationPolicy: &pb.ExpirationPolicy{},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"push_config", "expiration_policy"}},
	}
	pbSub, err := client.SubscriptionAdminClient.UpdateSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	_ = pbSub // TODO: Use protobuf subscription.
}

func ExampleSubscription_Update_pushConfigAuthenticationMethod() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	sub := "projects/project-id/subscriptions/subscription-id"
	req := &pb.UpdateSubscriptionRequest{
		Subscription: &pb.Subscription{
			Name: sub,
			PushConfig: &pb.PushConfig{
				PushEndpoint: "https://example.com/push",
				AuthenticationMethod: &pb.PushConfig_OidcToken_{
					OidcToken: &pb.PushConfig_OidcToken{
						ServiceAccountEmail: "service-account-email",
						Audience:            "client-12345",
					},
				},
			},
			ExpirationPolicy: &pb.ExpirationPolicy{},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"push_config"}},
	}
	pbSub, err := client.SubscriptionAdminClient.UpdateSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	_ = pbSub // TODO: Use protobuf subscription.
}

func ExampleSubscription_CreateSnapshot() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	subName := "projects/project-id/subscriptions/sub-id"
	req := &pb.CreateSnapshotRequest{
		Name:         "projects/project-id/snapshots/new-snapshot",
		Subscription: subName,
	}
	snapConfig, err := client.SubscriptionAdminClient.CreateSnapshot(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	_ = snapConfig // TODO: Use protobuf snapshot.
}

func ExampleSubscription_SeekToSnapshot() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	subName := "projects/project-id/subscriptions/sub-id"
	snapshotName := "projects/project-id/snapshots/snapshot-id"
	req := &pb.SeekRequest{
		Subscription: subName,
		Target: &pb.SeekRequest_Snapshot{
			Snapshot: snapshotName,
		},
	}
	_, err = client.SubscriptionAdminClient.Seek(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSubscription_SeekToTime() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	subName := "projects/project-id/subscriptions/sub-id"
	req := &pb.SeekRequest{
		Subscription: subName,
		Target: &pb.SeekRequest_Time{
			Time: timestamppb.New(time.Now().Add(-1 * time.Hour)),
		},
	}
	_, err = client.SubscriptionAdminClient.Seek(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSnapshot_Delete() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}

	snapshotName := "projects/project-id/snapshots/snapshot-id"
	err = client.SubscriptionAdminClient.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{Snapshot: snapshotName})
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_Snapshots() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	// List all snapshots for the project.
	req := &pb.ListSnapshotsRequest{
		Project: "project-id",
	}
	iter := client.SubscriptionAdminClient.ListSnapshots(ctx, req)
	_ = iter // TODO: iterate using Next.
}

func ExampleSnapshotConfigIterator_Next() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	// List all snapshots for the project.
	req := &pb.ListSnapshotsRequest{
		Project: "project-id",
	}
	iter := client.SubscriptionAdminClient.ListSnapshots(ctx, req)
	for {
		pbSnapshot, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		_ = pbSnapshot // TODO: use the protobuf Snapshot.
	}
}

// TODO(jba): write an example for PublishResult.Ready
// TODO(jba): write an example for Subscription.IAM
// TODO(jba): write an example for Topic.IAM
// TODO(jba): write an example for Topic.Stop
