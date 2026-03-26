package shutdown

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestAWSDetectorReturnsNotice(t *testing.T) {
	detector := newAWSDetector(Options{
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch request.URL.Path {
				case awsIMDSTokenPath:
					return responseWithBody(http.StatusOK, "token-1"), nil
				case awsSpotActionPath:
					if got := request.Header.Get("X-aws-ec2-metadata-token"); got != "token-1" {
						t.Fatalf("metadata token header = %q, want token-1", got)
					}
					return responseWithBody(http.StatusOK, `{"action":"terminate","time":"2026-03-26T10:00:00Z"}`), nil
				default:
					t.Fatalf("unexpected path %q", request.URL.Path)
					return nil, nil
				}
			}),
		},
	})

	notice, err := detector.Detect(context.Background())
	if err != nil {
		t.Fatalf("Detect() error = %v", err)
	}
	if notice == nil {
		t.Fatal("Detect() notice = nil, want value")
	}
	if notice.Provider != "aws" {
		t.Fatalf("Detect() provider = %q, want aws", notice.Provider)
	}
	if !strings.Contains(notice.Detail, "terminate") {
		t.Fatalf("Detect() detail = %q, want terminate", notice.Detail)
	}
	if got := notice.ShutdownAt.UTC().Format(time.RFC3339); got != "2026-03-26T10:00:00Z" {
		t.Fatalf("Detect() shutdown time = %q, want 2026-03-26T10:00:00Z", got)
	}
}

func TestGCPDetectorReturnsEstimatedNotice(t *testing.T) {
	detector := newGCPDetector(Options{
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if got := request.Header.Get(gcpMetadataFlavorHeader); got != "Google" {
					t.Fatalf("metadata flavor header = %q, want Google", got)
				}
				return responseWithBody(http.StatusOK, "TRUE"), nil
			}),
		},
	})

	before := time.Now().UTC()
	notice, err := detector.Detect(context.Background())
	if err != nil {
		t.Fatalf("Detect() error = %v", err)
	}
	if notice == nil {
		t.Fatal("Detect() notice = nil, want value")
	}
	if notice.Provider != "gcp" {
		t.Fatalf("Detect() provider = %q, want gcp", notice.Provider)
	}
	window := notice.ShutdownAt.Sub(before)
	if window < 25*time.Second || window > 35*time.Second {
		t.Fatalf("Detect() shutdown window = %s, want about 30s", window)
	}
}

func responseWithBody(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
