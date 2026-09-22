package engine

import (
	"context"
	"log/slog"
	"maps"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// MetricsOwner is implemented by recommenders that need metrics beyond the ones
// the policy declares. The engine registers the returned definitions on the
// policy under the recommender's name, so metric providers collect them like
// any other control metric while they stay scoped to their owner.
type MetricsOwner interface {
	// OwnedMetrics returns the metrics the recommender needs for this
	// definition. The returned definitions are owned by def.Name; the engine
	// stamps the owner on them.
	OwnedMetrics(def *pb.RecommenderDefinition) []*pb.MetricDefinition
}

// syncRecommenderMetrics registers the metrics owned by the recommenders of a
// policy.
//
// It takes the ScalingPolicy from the Server as the argument `policy`, and
// returns an updated version of this ScalingPolicy where all the recommender-owned
// metrics have been added.
//
// Only the metrics of the recommenders this binary manages (e.g. 'linear') are
// updated; others are left untouched.
func (e *Engine) syncRecommenderMetrics(policy *pb.Policy) *pb.Policy {
	owned := make(map[string]*pb.MetricDefinitionList)

	for _, recDef := range policy.Scaling {
		rec, err := e.recommenderFor(recDef.Recommender)
		if err != nil {
			continue
		}
		owner, ok := rec.(MetricsOwner)
		if !ok {
			continue
		}

		ownedMetrics := owner.OwnedMetrics(recDef)
		owned[recDef.Name] = &pb.MetricDefinitionList{Definitions: ownedMetrics}
	}

	updatedOwnedMetrics := maps.Clone(policy.RecommenderMetrics)
	if updatedOwnedMetrics == nil {
		updatedOwnedMetrics = make(map[string]*pb.MetricDefinitionList)
	}
	for name, list := range owned {
		if len(list.Definitions) > 0 {
			updatedOwnedMetrics[name] = list
		} else {
			delete(updatedOwnedMetrics, name)
		}
	}

	// Check if the Server's policy needs to be updated.
	if maps.EqualFunc(policy.RecommenderMetrics, updatedOwnedMetrics, func(a, b *pb.MetricDefinitionList) bool {
		return proto.Equal(a, b)
	}) {
		return policy
	}

	updated := proto.Clone(policy).(*pb.Policy)
	updated.RecommenderMetrics = updatedOwnedMetrics

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := e.client.UpdatePolicy(ctx, &pb.UpdatePolicyRequest{Policy: updated}); err != nil {
		slog.Error("Failed to register recommender owned metrics", "policy", policy.Id.Name, "error", err)
		return policy
	}
	slog.Debug("Registered recommender owned metrics", "policy", policy.Id.Name, "recommenders", len(updatedOwnedMetrics))
	return updated
}
