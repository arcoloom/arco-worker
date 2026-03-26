package shutdown

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	awsMetadataBaseURL  = "http://169.254.169.254"
	awsIMDSTokenPath    = "/latest/api/token"
	awsSpotActionPath   = "/latest/meta-data/spot/instance-action"
	awsTokenTTLSeconds  = 21600
	awsTokenRefreshSkew = 5 * time.Minute
)

type awsDetector struct {
	client *http.Client

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
}

type awsInstanceAction struct {
	Action string `json:"action"`
	Time   string `json:"time"`
}

func newAWSDetector(options Options) Detector {
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &awsDetector{client: client}
}

func (d *awsDetector) Detect(ctx context.Context) (*Notice, error) {
	token, err := d.metadataToken(ctx)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, awsMetadataBaseURL+awsSpotActionPath, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-aws-ec2-metadata-token", token)

	response, err := d.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusNotFound:
		return nil, nil
	case http.StatusUnauthorized:
		d.clearToken()
		return nil, errors.New("aws metadata token expired")
	case http.StatusOK:
	default:
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return nil, fmt.Errorf("aws spot metadata returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	var action awsInstanceAction
	if err := json.NewDecoder(response.Body).Decode(&action); err != nil {
		return nil, fmt.Errorf("decode aws spot interruption notice: %w", err)
	}
	shutdownAt, err := time.Parse(time.RFC3339, strings.TrimSpace(action.Time))
	if err != nil {
		return nil, fmt.Errorf("parse aws interruption time %q: %w", action.Time, err)
	}

	detail := strings.TrimSpace(action.Action)
	if detail == "" {
		detail = "terminate"
	}
	return &Notice{
		Provider:   "aws",
		Detail:     fmt.Sprintf("aws spot interruption notice: action=%s", detail),
		ShutdownAt: shutdownAt.UTC(),
	}, nil
}

func (d *awsDetector) metadataToken(ctx context.Context) (string, error) {
	d.mu.Lock()
	token := d.token
	expiry := d.tokenExpiry
	d.mu.Unlock()

	if token != "" && time.Until(expiry) > awsTokenRefreshSkew {
		return token, nil
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, awsMetadataBaseURL+awsIMDSTokenPath, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", fmt.Sprintf("%d", awsTokenTTLSeconds))

	response, err := d.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return "", fmt.Errorf("aws imds token request returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return "", err
	}
	token = strings.TrimSpace(string(body))
	if token == "" {
		return "", errors.New("aws imds returned an empty token")
	}

	d.mu.Lock()
	d.token = token
	d.tokenExpiry = time.Now().Add(awsTokenTTLSeconds * time.Second)
	d.mu.Unlock()
	return token, nil
}

func (d *awsDetector) clearToken() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.token = ""
	d.tokenExpiry = time.Time{}
}
