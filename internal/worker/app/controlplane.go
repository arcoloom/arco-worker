package app

import (
	"context"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
)

type ControlPlaneSession interface {
	Send(context.Context, *workerv1.WorkerToControl) error
	Receive(context.Context) (*workerv1.ControlToWorker, error)
	CloseSend() error
}

type ControlPlaneClient interface {
	Connect(context.Context) (ControlPlaneSession, error)
}

type GRPCControlPlaneClient struct {
	client workerv1.WorkerServiceClient
}

func NewGRPCControlPlaneClient(client workerv1.WorkerServiceClient) *GRPCControlPlaneClient {
	return &GRPCControlPlaneClient{client: client}
}

func (c *GRPCControlPlaneClient) Connect(ctx context.Context) (ControlPlaneSession, error) {
	stream, err := c.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &grpcControlPlaneSession{stream: stream}, nil
}

type grpcBidirectionalStream interface {
	Send(*workerv1.WorkerToControl) error
	Recv() (*workerv1.ControlToWorker, error)
	CloseSend() error
}

type grpcControlPlaneSession struct {
	stream grpcBidirectionalStream
}

func (s *grpcControlPlaneSession) Send(_ context.Context, message *workerv1.WorkerToControl) error {
	return s.stream.Send(message)
}

func (s *grpcControlPlaneSession) Receive(_ context.Context) (*workerv1.ControlToWorker, error) {
	return s.stream.Recv()
}

func (s *grpcControlPlaneSession) CloseSend() error {
	return s.stream.CloseSend()
}
