package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveCloudMetadataAWS(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case awsIMDSTokenPath:
			if request.Method != http.MethodPut {
				t.Fatalf("token method = %s, want PUT", request.Method)
			}
			if got := request.Header.Get("X-aws-ec2-metadata-token-ttl-seconds"); got == "" {
				t.Fatal("aws token ttl header was not set")
			}
			_, _ = writer.Write([]byte("token-1"))
		case awsInstanceIdentityDocPath:
			if request.Method != http.MethodGet {
				t.Fatalf("document method = %s, want GET", request.Method)
			}
			if got := request.Header.Get("X-aws-ec2-metadata-token"); got != "token-1" {
				t.Fatalf("metadata token = %q, want %q", got, "token-1")
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{
				"instanceType": "c7g.medium",
				"region": "us-west-2",
				"availabilityZone": "us-west-2b"
			}`))
		default:
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	metadata, err := resolveCloudMetadata(context.Background(), "aws", cloudMetadataResolverOptions{
		HTTPClient: server.Client(),
		AWSBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("resolveCloudMetadata() error = %v", err)
	}

	if metadata.CloudVendor != "aws" {
		t.Fatalf("cloud vendor = %q, want %q", metadata.CloudVendor, "aws")
	}
	if metadata.InstanceType != "c7g.medium" {
		t.Fatalf("instance type = %q, want %q", metadata.InstanceType, "c7g.medium")
	}
	if metadata.Region != "us-west-2" {
		t.Fatalf("region = %q, want %q", metadata.Region, "us-west-2")
	}
	if metadata.AvailabilityZone != "us-west-2b" {
		t.Fatalf("availability zone = %q, want %q", metadata.AvailabilityZone, "us-west-2b")
	}
}

func TestResolveCloudMetadataGCP(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get(gcpMetadataFlavorHeader); got != "Google" {
			t.Fatalf("metadata flavor header = %q, want %q", got, "Google")
		}
		switch request.URL.Path {
		case gcpMachineTypePath:
			_, _ = writer.Write([]byte("projects/123456/machineTypes/c4-standard-4"))
		case gcpZonePath:
			_, _ = writer.Write([]byte("projects/123456/zones/us-central1-a"))
		default:
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	metadata, err := resolveCloudMetadata(context.Background(), "gcp", cloudMetadataResolverOptions{
		HTTPClient: server.Client(),
		GCPBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("resolveCloudMetadata() error = %v", err)
	}

	if metadata.CloudVendor != "gcp" {
		t.Fatalf("cloud vendor = %q, want %q", metadata.CloudVendor, "gcp")
	}
	if metadata.InstanceType != "c4-standard-4" {
		t.Fatalf("instance type = %q, want %q", metadata.InstanceType, "c4-standard-4")
	}
	if metadata.Region != "us-central1" {
		t.Fatalf("region = %q, want %q", metadata.Region, "us-central1")
	}
	if metadata.AvailabilityZone != "us-central1-a" {
		t.Fatalf("availability zone = %q, want %q", metadata.AvailabilityZone, "us-central1-a")
	}
}
