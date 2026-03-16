package app

import (
	"context"

	workerv1 "github.com/arcoloom/arco-worker/gen/proto/arcoloom/worker/v1"
)

// ControlPlaneClient hides the transport-specific gRPC client from the worker runner.
type ControlPlaneClient interface {
	Register(ctx context.Context, req *workerv1.RegisterRequest) (*workerv1.RegisterResponse, error)
	ReportStatus(ctx context.Context, req *workerv1.ReportStatusRequest) error
	ReportSignal(ctx context.Context, req *workerv1.ReportSignalRequest) error
}

// GRPCControlPlaneClient adapts the generated WorkerService client to ControlPlaneClient.
type GRPCControlPlaneClient struct {
	client workerv1.WorkerServiceClient
}

// NewGRPCControlPlaneClient constructs a control-plane client backed by gRPC.
func NewGRPCControlPlaneClient(client workerv1.WorkerServiceClient) *GRPCControlPlaneClient {
	return &GRPCControlPlaneClient{client: client}
}

// Register registers the worker and receives the task assignment.
func (c *GRPCControlPlaneClient) Register(ctx context.Context, req *workerv1.RegisterRequest) (*workerv1.RegisterResponse, error) {
	return c.client.Register(ctx, req)
}

// ReportStatus reports task lifecycle status to the control plane.
func (c *GRPCControlPlaneClient) ReportStatus(ctx context.Context, req *workerv1.ReportStatusRequest) error {
	_, err := c.client.ReportStatus(ctx, req)
	return err
}

// ReportSignal reports infrastructure or host-level signals to the control plane.
func (c *GRPCControlPlaneClient) ReportSignal(ctx context.Context, req *workerv1.ReportSignalRequest) error {
	_, err := c.client.ReportSignal(ctx, req)
	return err
}
