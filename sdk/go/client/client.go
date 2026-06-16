package client

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/beranekio/responses-api-store/sdk/go/responsesapistore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// DefaultMaxMessageBytes is the default gRPC send/recv limit (64 MiB).
// The gRPC library default is 4 MiB, which is too small for large Responses API payloads.
const DefaultMaxMessageBytes = 64 * 1024 * 1024

// StoredResponse mirrors the gateway-owned record used by Responses API compatible services.
type StoredResponse struct {
	Upstream               string            `json:"upstream"`
	Response               json.RawMessage   `json:"response"`
	Input                  []json.RawMessage `json:"input"`
	PendingUpstreamRequest json.RawMessage   `json:"pending_upstream_request,omitempty"`
	UpstreamAuthorization  string            `json:"upstream_authorization,omitempty"`
	EnqueuedAt             *int64            `json:"enqueued_at,omitempty"`
}

// BackgroundJob is a claimed background queue message with its stored record.
type BackgroundJob struct {
	StreamID    string
	ResponseID  string
	Record      StoredResponse
	Autoclaimed bool
	// Minimum idle time in milliseconds when autoclaimed (from autoclaim_min_idle_ms).
	IdleMS *uint64
}

// PendingBackgroundJob is a claimed stream entry that could not be hydrated.
type PendingBackgroundJob struct {
	StreamID    string
	ResponseID  string
	Autoclaimed bool
	// Minimum idle time in milliseconds when autoclaimed (from autoclaim_min_idle_ms).
	IdleMS *uint64
}

// Client wraps the generated gRPC client with JSON-friendly helpers.
type Client struct {
	conn   *grpc.ClientConn
	client pb.ResponsesApiStoreClient
}

// Dial connects to the Responses API store gRPC endpoint.
func Dial(ctx context.Context, target string, opts ...grpc.DialOption) (*Client, error) {
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultMaxMessageBytes),
			grpc.MaxCallSendMsgSize(DefaultMaxMessageBytes),
		),
	}, opts...)
	conn, err := grpc.DialContext(ctx, target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial responses api store: %w", err)
	}
	return &Client{
		conn:   conn,
		client: pb.NewResponsesApiStoreClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// GRPC returns the underlying generated client.
func (c *Client) GRPC() pb.ResponsesApiStoreClient {
	return c.client
}

// Health checks service and Redis connectivity.
func (c *Client) Health(ctx context.Context) (*pb.HealthResponse, error) {
	return c.client.Health(ctx, &pb.HealthRequest{})
}

// GenerateResponseID allocates a new resp_* identifier.
func (c *Client) GenerateResponseID(ctx context.Context) (string, error) {
	resp, err := c.client.GenerateResponseId(ctx, &pb.GenerateResponseIdRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetResponseId(), nil
}

// StoreResponse persists a stored response record.
func (c *Client) StoreResponse(ctx context.Context, responseID string, record StoredResponse, ttlSeconds uint64) error {
	protoRecord, err := toProtoRecord(responseID, record)
	if err != nil {
		return err
	}
	_, err = c.client.StoreResponse(ctx, &pb.StoreResponseRequest{
		Record:     protoRecord,
		TtlSeconds: ttlSeconds,
	})
	return err
}

// GetResponse loads a stored response record.
func (c *Client) GetResponse(ctx context.Context, responseID string, reconcileStale bool) (StoredResponse, error) {
	resp, err := c.client.GetResponse(ctx, &pb.GetResponseRequest{
		ResponseId:     responseID,
		ReconcileStale: reconcileStale,
	})
	if err != nil {
		return StoredResponse{}, err
	}
	return fromProtoRecord(resp.GetRecord())
}

// UpdateResponse replaces a stored response record.
func (c *Client) UpdateResponse(ctx context.Context, responseID string, record StoredResponse, ttlSeconds uint64) error {
	protoRecord, err := toProtoRecord(responseID, record)
	if err != nil {
		return err
	}
	_, err = c.client.UpdateResponse(ctx, &pb.UpdateResponseRequest{
		Record:     protoRecord,
		TtlSeconds: ttlSeconds,
	})
	return err
}

// DeleteResponse deletes or tombstones a stored response.
func (c *Client) DeleteResponse(ctx context.Context, responseID string) error {
	_, err := c.client.DeleteResponse(ctx, &pb.DeleteResponseRequest{ResponseId: responseID})
	return err
}

// EnqueueBackgroundJob stores a record and enqueues it for background workers.
func (c *Client) EnqueueBackgroundJob(ctx context.Context, responseID string, record StoredResponse) error {
	protoRecord, err := toProtoRecord(responseID, record)
	if err != nil {
		return err
	}
	_, err = c.client.EnqueueBackgroundJob(ctx, &pb.EnqueueBackgroundJobRequest{Record: protoRecord})
	return err
}

// ClaimBackgroundJobsResult is the outcome of a background job claim batch.
type ClaimBackgroundJobsResult struct {
	Jobs             []BackgroundJob
	PendingStreamIDs []string
	PendingJobs      []PendingBackgroundJob
}

// ClaimBackgroundJobs claims one or more jobs from the background queue.
func (c *Client) ClaimBackgroundJobs(ctx context.Context, req *pb.ClaimBackgroundJobsRequest) (ClaimBackgroundJobsResult, error) {
	resp, err := c.client.ClaimBackgroundJobs(ctx, req)
	if err != nil {
		return ClaimBackgroundJobsResult{}, err
	}

	jobs := make([]BackgroundJob, 0, len(resp.GetJobs()))
	for _, job := range resp.GetJobs() {
		if job == nil {
			continue
		}
		record, err := fromProtoRecord(job.GetRecord())
		if err != nil {
			return ClaimBackgroundJobsResult{}, err
		}
		jobs = append(jobs, BackgroundJob{
			StreamID:    job.GetStreamId(),
			ResponseID:  job.GetResponseId(),
			Record:      record,
			Autoclaimed: job.GetAutoclaimed(),
			IdleMS:      job.IdleMs,
		})
	}
	pendingJobs := make([]PendingBackgroundJob, 0, len(resp.GetPendingJobs()))
	for _, pending := range resp.GetPendingJobs() {
		if pending == nil {
			continue
		}
		pendingJobs = append(pendingJobs, PendingBackgroundJob{
			StreamID:    pending.GetStreamId(),
			ResponseID:  pending.GetResponseId(),
			Autoclaimed: pending.GetAutoclaimed(),
			IdleMS:      pending.IdleMs,
		})
	}
	return ClaimBackgroundJobsResult{
		Jobs:             jobs,
		PendingStreamIDs: resp.GetPendingStreamIds(),
		PendingJobs:      pendingJobs,
	}, nil
}

// AcknowledgeBackgroundJob acknowledges successful processing of a queue message.
func (c *Client) AcknowledgeBackgroundJob(ctx context.Context, consumerGroup, streamID string) error {
	_, err := c.client.AcknowledgeBackgroundJob(ctx, &pb.AcknowledgeBackgroundJobRequest{
		StreamId:      streamID,
		ConsumerGroup: consumerGroup,
	})
	return err
}

// BackgroundQueueStats is a store-agnostic queue depth signal for autoscaling.
type BackgroundQueueStats struct {
	Pending    uint64
	InProgress uint64
	Workload   uint64
}

// GetBackgroundQueueStats returns pending, in-progress, and workload counts for a consumer group.
func (c *Client) GetBackgroundQueueStats(ctx context.Context, consumerGroup string) (BackgroundQueueStats, error) {
	resp, err := c.client.GetBackgroundQueueStats(ctx, &pb.GetBackgroundQueueStatsRequest{
		ConsumerGroup: consumerGroup,
	})
	if err != nil {
		return BackgroundQueueStats{}, err
	}
	return BackgroundQueueStats{
		Pending:    resp.GetPending(),
		InProgress: resp.GetInProgress(),
		Workload:   resp.GetWorkload(),
	}, nil
}

// EnsureConsumerGroup creates the Redis stream consumer group when missing.
func (c *Client) EnsureConsumerGroup(ctx context.Context, consumerGroup, startID string) (bool, error) {
	resp, err := c.client.EnsureConsumerGroup(ctx, &pb.EnsureConsumerGroupRequest{
		ConsumerGroup: consumerGroup,
		StartId:       startID,
	})
	if err != nil {
		return false, err
	}
	return resp.GetCreated(), nil
}

// ReconcileStaleResponse marks stale queued background responses as failed when applicable.
func (c *Client) ReconcileStaleResponse(ctx context.Context, responseID string, staleSeconds int64) (StoredResponse, bool, error) {
	resp, err := c.client.ReconcileStaleResponse(ctx, &pb.ReconcileStaleResponseRequest{
		ResponseId:   responseID,
		StaleSeconds: staleSeconds,
	})
	if err != nil {
		return StoredResponse{}, false, err
	}
	record, err := fromProtoRecord(resp.GetRecord())
	if err != nil {
		return StoredResponse{}, false, err
	}
	return record, resp.GetReconciled(), nil
}

// IsNotFound reports whether err is a gRPC not-found error.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == 5 // codes.NotFound
}

func toProtoRecord(responseID string, record StoredResponse) (*pb.StoredResponse, error) {
	inputJSON := make([]string, 0, len(record.Input))
	for _, item := range record.Input {
		inputJSON = append(inputJSON, string(item))
	}

	var pending string
	if len(record.PendingUpstreamRequest) > 0 {
		pending = string(record.PendingUpstreamRequest)
	}

	return &pb.StoredResponse{
		ResponseId:                 responseID,
		Upstream:                   record.Upstream,
		ResponseJson:               string(record.Response),
		InputJson:                  inputJSON,
		PendingUpstreamRequestJson: optionalString(pending),
		UpstreamAuthorization:      optionalString(record.UpstreamAuthorization),
		EnqueuedAt:                 record.EnqueuedAt,
	}, nil
}

func fromProtoRecord(record *pb.StoredResponse) (StoredResponse, error) {
	if record == nil {
		return StoredResponse{}, fmt.Errorf("missing stored response record")
	}

	input := make([]json.RawMessage, 0, len(record.GetInputJson()))
	for _, item := range record.GetInputJson() {
		input = append(input, json.RawMessage(item))
	}

	var pending json.RawMessage
	if record.PendingUpstreamRequestJson != nil && *record.PendingUpstreamRequestJson != "" {
		pending = json.RawMessage(*record.PendingUpstreamRequestJson)
	}

	return StoredResponse{
		Upstream:               record.GetUpstream(),
		Response:               json.RawMessage(record.GetResponseJson()),
		Input:                  input,
		PendingUpstreamRequest: pending,
		UpstreamAuthorization:  record.GetUpstreamAuthorization(),
		EnqueuedAt:             record.EnqueuedAt,
	}, nil
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
