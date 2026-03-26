package shutdown

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	gcpMetadataURL          = "http://metadata.google.internal/computeMetadata/v1/instance/preempted"
	gcpMetadataFlavorHeader = "Metadata-Flavor"
	gcpPreemptionWindow     = 30 * time.Second
)

type gcpDetector struct {
	client *http.Client
}

func newGCPDetector(options Options) Detector {
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &gcpDetector{client: client}
}

func (d *gcpDetector) Detect(ctx context.Context) (*Notice, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, gcpMetadataURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set(gcpMetadataFlavorHeader, "Google")

	response, err := d.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return nil, fmt.Errorf("gcp metadata returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, 1024))
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(strings.TrimSpace(string(body)), "TRUE") {
		return nil, nil
	}

	now := time.Now().UTC()
	return &Notice{
		Provider:   "gcp",
		Detail:     "gcp spot preemption notice",
		ShutdownAt: now.Add(gcpPreemptionWindow),
	}, nil
}
