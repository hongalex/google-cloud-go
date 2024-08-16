// Copyright 2016 Google LLC
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

package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	ipubsub "cloud.google.com/go/internal/pubsub"
	admingen "cloud.google.com/go/pubsub/admingen"
	pb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/internal/scheduler"
	gax "github.com/googleapis/gax-go/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/protobuf/proto"
)

const (
	// MaxPublishRequestCount is the maximum number of messages that can be in
	// a single publish request, as defined by the PubSub service.
	MaxPublishRequestCount = 1000

	// MaxPublishRequestBytes is the maximum size of a single publish request
	// in bytes, as defined by the PubSub service.
	MaxPublishRequestBytes = 1e7
)

const (
	// TODO: math.MaxInt was added in Go 1.17. We should use that once 1.17
	// becomes the minimum supported version of Go.
	intSize = 32 << (^uint(0) >> 63)
	maxInt  = 1<<(intSize-1) - 1
)

// ErrOversizedMessage indicates that a message's size exceeds MaxPublishRequestBytes.
var ErrOversizedMessage = bundler.ErrOversizedItem

// Publisher is a reference to a PubSub topic.
//
// The methods of Publisher are safe for use by multiple goroutines.
type Publisher struct {
	Admin *admingen.TopicAdminClient

	c *Client
	// The fully qualified identifier for the topic, in the format "projects/<projid>/topics/<name>"
	name string

	// Settings for publishing messages. All changes must be made before the
	// first call to Publish. The default is DefaultPublishSettings.
	PublishSettings PublishSettings

	mu        sync.RWMutex
	stopped   bool
	scheduler *scheduler.PublishScheduler

	flowController

	// EnableMessageOrdering enables delivery of ordered keys.
	EnableMessageOrdering bool
}

// PublishSettings control the bundling of published messages.
type PublishSettings struct {

	// Publish a non-empty batch after this delay has passed.
	DelayThreshold time.Duration

	// Publish a batch when it has this many messages. The maximum is
	// MaxPublishRequestCount.
	CountThreshold int

	// Publish a batch when its size in bytes reaches this value.
	ByteThreshold int

	// The number of goroutines used in each of the data structures that are
	// involved along the the Publish path. Adjusting this value adjusts
	// concurrency along the publish path.
	//
	// Defaults to a multiple of GOMAXPROCS.
	NumGoroutines int

	// The maximum time that the client will attempt to publish a bundle of messages.
	Timeout time.Duration

	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow. This is now superseded by FlowControlSettings.MaxOutstandingBytes.
	// If MaxOutstandingBytes is set, that value will override BufferedByteLimit.
	//
	// Defaults to DefaultPublishSettings.BufferedByteLimit.
	// Deprecated: Set `Topic.PublishSettings.FlowControlSettings.MaxOutstandingBytes` instead.
	BufferedByteLimit int

	// FlowControlSettings defines publisher flow control settings.
	FlowControlSettings FlowControlSettings

	// EnableCompression enables transport compression for Publish operations
	EnableCompression bool

	// CompressionBytesThreshold defines the threshold (in bytes) above which messages
	// are compressed for transport. Only takes effect if EnableCompression is true.
	CompressionBytesThreshold int
}

func (ps *PublishSettings) shouldCompress(batchSize int) bool {
	return ps.EnableCompression && batchSize > ps.CompressionBytesThreshold
}

// DefaultPublishSettings holds the default values for topics' PublishSettings.
var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        60 * time.Second,
	// By default, limit the bundler to 10 times the max message size. The number 10 is
	// chosen as a reasonable amount of messages in the worst case whilst still
	// capping the number to a low enough value to not OOM users.
	BufferedByteLimit: 10 * MaxPublishRequestBytes,
	FlowControlSettings: FlowControlSettings{
		MaxOutstandingMessages: 1000,
		MaxOutstandingBytes:    -1,
		LimitExceededBehavior:  FlowControlIgnore,
	},
	// Publisher compression defaults matches Java's defaults
	// https://github.com/googleapis/java-pubsub/blob/7d33e7891db1b2e32fd523d7655b6c11ea140a8b/google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/Publisher.java#L717-L718
	EnableCompression:         false,
	CompressionBytesThreshold: 240,
}

// Publisher creates a reference to a publisher in the client's project.
//
// If Publisher.Publish method is called, it has background goroutines
// associated with it. Clean them up by calling Topic.Stop.
//
// Avoid creating many Topic instances if you use them to publish.
func (c *Client) Publisher(id string) *Publisher {
	return c.PublisherInProject(id, c.projectID)
}

// PublisherInProject creates a reference to a publisher in the given project.
//
// If a Publisher's Publish method is called, it has background goroutines
// associated with it. Clean them up by calling Topic.Stop.
//
// Avoid creating many Publisher instances.
func (c *Client) PublisherInProject(id, projectID string) *Publisher {
	return newPublisher(c, fmt.Sprintf("projects/%s/topics/%s", projectID, id))
}

func newPublisher(c *Client, name string) *Publisher {
	return &Publisher{
		c:               c,
		name:            name,
		PublishSettings: DefaultPublishSettings,
	}
}

// TopicState denotes the possible states for a topic.
type TopicState int

const (
	// TopicStateUnspecified is the default value. This value is unused.
	TopicStateUnspecified = iota

	// TopicStateActive means the topic does not have any persistent errors.
	TopicStateActive

	// TopicStateIngestionResourceError means ingestion from the data source
	// has encountered a permanent error.
	// See the more detailed error state in the corresponding ingestion
	// source configuration.
	TopicStateIngestionResourceError
)

// ID returns the unique identifier of the topic within its project.
func (t *Publisher) ID() string {
	slash := strings.LastIndex(t.name, "/")
	if slash == -1 {
		// name is not a fully-qualified name.
		panic("bad topic name")
	}
	return t.name[slash+1:]
}

// String returns the printable globally unique name for the publisher.
func (t *Publisher) String() string {
	return t.name
}

// ErrTopicStopped indicates that topic has been stopped and further publishing will fail.
var ErrTopicStopped = errors.New("pubsub: Stop has been called for this topic")

// A PublishResult holds the result from a call to Publish.
//
// Call Get to obtain the result of the Publish call. Example:
//
//	// Get blocks until Publish completes or ctx is done.
//	id, err := r.Get(ctx)
//	if err != nil {
//	    // TODO: Handle error.
//	}
type PublishResult = ipubsub.PublishResult

var errTopicOrderingNotEnabled = errors.New("Topic.EnableMessageOrdering=false, but an OrderingKey was set in Message. Please remove the OrderingKey or turn on Topic.EnableMessageOrdering")

// Publish publishes msg to the topic asynchronously. Messages are batched and
// sent according to the topic's PublishSettings. Publish never blocks.
//
// Publish returns a non-nil PublishResult which will be ready when the
// message has been sent (or has failed to be sent) to the server.
//
// Publish creates goroutines for batching and sending messages. These goroutines
// need to be stopped by calling t.Stop(). Once stopped, future calls to Publish
// will immediately return a PublishResult with an error.
func (t *Publisher) Publish(ctx context.Context, msg *Message) *PublishResult {
	ctx, err := tag.New(ctx, tag.Insert(keyStatus, "OK"), tag.Upsert(keyTopic, t.name))
	if err != nil {
		log.Printf("pubsub: cannot create context with tag in Publish: %v", err)
	}

	r := ipubsub.NewPublishResult()
	if !t.EnableMessageOrdering && msg.OrderingKey != "" {
		ipubsub.SetPublishResult(r, "", errTopicOrderingNotEnabled)
		return r
	}

	// Calculate the size of the encoded proto message by accounting
	// for the length of an individual PubSubMessage and Data/Attributes field.
	msgSize := proto.Size(&pb.PubsubMessage{
		Data:        msg.Data,
		Attributes:  msg.Attributes,
		OrderingKey: msg.OrderingKey,
	})

	t.initBundler()
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.stopped {
		ipubsub.SetPublishResult(r, "", ErrTopicStopped)
		return r
	}

	if err := t.flowController.acquire(ctx, msgSize); err != nil {
		t.scheduler.Pause(msg.OrderingKey)
		ipubsub.SetPublishResult(r, "", err)
		return r
	}
	err = t.scheduler.Add(msg.OrderingKey, &bundledMessage{msg, r, msgSize}, msgSize)
	if err != nil {
		t.scheduler.Pause(msg.OrderingKey)
		ipubsub.SetPublishResult(r, "", err)
	}
	return r
}

// Stop sends all remaining published messages and stop goroutines created for handling
// publishing. Returns once all outstanding messages have been sent or have
// failed to be sent.
func (t *Publisher) Stop() {
	t.mu.Lock()
	noop := t.stopped || t.scheduler == nil
	t.stopped = true
	t.mu.Unlock()
	if noop {
		return
	}
	t.scheduler.FlushAndStop()
}

// Flush blocks until all remaining messages are sent.
func (t *Publisher) Flush() {
	if t.stopped || t.scheduler == nil {
		return
	}
	t.scheduler.Flush()
}

type bundledMessage struct {
	msg  *Message
	res  *PublishResult
	size int
}

func (t *Publisher) initBundler() {
	t.mu.RLock()
	noop := t.stopped || t.scheduler != nil
	t.mu.RUnlock()
	if noop {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Must re-check, since we released the lock.
	if t.stopped || t.scheduler != nil {
		return
	}

	timeout := t.PublishSettings.Timeout

	workers := t.PublishSettings.NumGoroutines
	// Unless overridden, allow many goroutines per CPU to call the Publish RPC
	// concurrently. The default value was determined via extensive load
	// testing (see the loadtest subdirectory).
	if t.PublishSettings.NumGoroutines == 0 {
		workers = 25 * runtime.GOMAXPROCS(0)
	}

	t.scheduler = scheduler.NewPublishScheduler(workers, func(bundle interface{}) {
		// TODO(jba): use a context detached from the one passed to NewClient.
		ctx := context.TODO()
		if timeout != 0 {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		t.publishMessageBundle(ctx, bundle.([]*bundledMessage))
	})
	t.scheduler.DelayThreshold = t.PublishSettings.DelayThreshold
	t.scheduler.BundleCountThreshold = t.PublishSettings.CountThreshold
	if t.scheduler.BundleCountThreshold > MaxPublishRequestCount {
		t.scheduler.BundleCountThreshold = MaxPublishRequestCount
	}
	t.scheduler.BundleByteThreshold = t.PublishSettings.ByteThreshold

	fcs := DefaultPublishSettings.FlowControlSettings
	fcs.LimitExceededBehavior = t.PublishSettings.FlowControlSettings.LimitExceededBehavior
	if t.PublishSettings.FlowControlSettings.MaxOutstandingBytes > 0 {
		b := t.PublishSettings.FlowControlSettings.MaxOutstandingBytes
		fcs.MaxOutstandingBytes = b

		// If MaxOutstandingBytes is set, disable BufferedByteLimit by setting it to maxint.
		// This is because there's no way to set "unlimited" for BufferedByteLimit,
		// and simply setting it to MaxOutstandingBytes occasionally leads to issues where
		// BufferedByteLimit is reached even though there are resources available.
		t.PublishSettings.BufferedByteLimit = maxInt
	}
	if t.PublishSettings.FlowControlSettings.MaxOutstandingMessages > 0 {
		fcs.MaxOutstandingMessages = t.PublishSettings.FlowControlSettings.MaxOutstandingMessages
	}

	t.flowController = newTopicFlowController(fcs)

	bufferedByteLimit := DefaultPublishSettings.BufferedByteLimit
	if t.PublishSettings.BufferedByteLimit > 0 {
		bufferedByteLimit = t.PublishSettings.BufferedByteLimit
	}
	t.scheduler.BufferedByteLimit = bufferedByteLimit

	// Calculate the max limit of a single bundle. 5 comes from the number of bytes
	// needed to be reserved for encoding the PubsubMessage repeated field.
	t.scheduler.BundleByteLimit = MaxPublishRequestBytes - calcFieldSizeString(t.name) - 5
}

// ErrPublishingPaused is a custom error indicating that the publish paused for the specified ordering key.
type ErrPublishingPaused struct {
	OrderingKey string
}

func (e ErrPublishingPaused) Error() string {
	return fmt.Sprintf("pubsub: Publishing for ordering key, %s, paused due to previous error. Call topic.ResumePublish(orderingKey) before resuming publishing", e.OrderingKey)

}

func (t *Publisher) publishMessageBundle(ctx context.Context, bms []*bundledMessage) {
	ctx, err := tag.New(ctx, tag.Insert(keyStatus, "OK"), tag.Upsert(keyTopic, t.name))
	if err != nil {
		log.Printf("pubsub: cannot create context with tag in publishMessageBundle: %v", err)
	}
	pbMsgs := make([]*pb.PubsubMessage, len(bms))
	var orderingKey string
	batchSize := 0
	for i, bm := range bms {
		orderingKey = bm.msg.OrderingKey
		pbMsgs[i] = &pb.PubsubMessage{
			Data:        bm.msg.Data,
			Attributes:  bm.msg.Attributes,
			OrderingKey: bm.msg.OrderingKey,
		}
		batchSize = batchSize + proto.Size(pbMsgs[i])
		bm.msg = nil // release bm.msg for GC
	}
	var res *pb.PublishResponse
	start := time.Now()
	if orderingKey != "" && t.scheduler.IsPaused(orderingKey) {
		err = ErrPublishingPaused{OrderingKey: orderingKey}
	} else {
		// Apply custom publish retryer on top of user specified retryer and
		// default retryer.
		opts := t.c.pubc.CallOptions.Publish
		var settings gax.CallSettings
		for _, opt := range opts {
			opt.Resolve(&settings)
		}
		r := &publishRetryer{defaultRetryer: settings.Retry()}
		gaxOpts := []gax.CallOption{
			gax.WithGRPCOptions(grpc.MaxCallSendMsgSize(maxSendRecvBytes)),
			gax.WithRetry(func() gax.Retryer { return r }),
		}
		if t.PublishSettings.shouldCompress(batchSize) {
			gaxOpts = append(gaxOpts, gax.WithGRPCOptions(grpc.UseCompressor(gzip.Name)))
		}
		res, err = t.c.pubc.Publish(ctx, &pb.PublishRequest{
			Topic:    t.name,
			Messages: pbMsgs,
		}, gaxOpts...)
	}
	end := time.Now()
	if err != nil {
		t.scheduler.Pause(orderingKey)
		// Update context with error tag for OpenCensus,
		// using same stats.Record() call as success case.
		ctx, _ = tag.New(ctx, tag.Upsert(keyStatus, "ERROR"),
			tag.Upsert(keyError, err.Error()))
	}
	stats.Record(ctx,
		PublishLatency.M(float64(end.Sub(start)/time.Millisecond)),
		PublishedMessages.M(int64(len(bms))))
	for i, bm := range bms {
		t.flowController.release(ctx, bm.size)
		if err != nil {
			ipubsub.SetPublishResult(bm.res, "", err)
		} else {
			ipubsub.SetPublishResult(bm.res, res.MessageIds[i], nil)
		}
	}
}

// ResumePublish resumes accepting messages for the provided ordering key.
// Publishing using an ordering key might be paused if an error is
// encountered while publishing, to prevent messages from being published
// out of order.
func (t *Publisher) ResumePublish(orderingKey string) {
	t.mu.RLock()
	noop := t.scheduler == nil
	t.mu.RUnlock()
	if noop {
		return
	}

	t.scheduler.Resume(orderingKey)
}
