// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"cloud.google.com/go/iam"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

func (c *TopicAdminClient) SubscriptionIAM(subscription *pubsubpb.Subscription) *iam.Handle {
	return iam.InternalNewHandle(c.Connection(), subscription.Name)
}

func (c *TopicAdminClient) TopicIAM(topic *pubsubpb.Topic) *iam.Handle {
	return iam.InternalNewHandle(c.Connection(), topic.Name)
}

func (c *SubscriptionAdminClient) SubscriptionIAM(subscription *pubsubpb.Subscription) *iam.Handle {
	return iam.InternalNewHandle(c.Connection(), subscription.Name)
}

func (c *SubscriptionAdminClient) TopicIAM(topic *pubsubpb.Topic) *iam.Handle {
	return iam.InternalNewHandle(c.Connection(), topic.Name)
}