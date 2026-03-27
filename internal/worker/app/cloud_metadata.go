package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	awsMetadataBaseURL         = "http://169.254.169.254"
	awsIMDSTokenPath           = "/latest/api/token"
	awsInstanceIdentityDocPath = "/latest/dynamic/instance-identity/document"
	awsMetadataTokenTTLSeconds = 60
	gcpMetadataBaseURL         = "http://metadata.google.internal"
	gcpMachineTypePath         = "/computeMetadata/v1/instance/machine-type"
	gcpZonePath                = "/computeMetadata/v1/instance/zone"
	gcpMetadataFlavorHeader    = "Metadata-Flavor"
)

type CloudMetadata struct {
	CloudVendor      string
	InstanceType     string
	Region           string
	AvailabilityZone string
}

type cloudMetadataResolverOptions struct {
	HTTPClient *http.Client
	AWSBaseURL string
	GCPBaseURL string
}

type awsInstanceIdentityDocument struct {
	InstanceType     string `json:"instanceType"`
	Region           string `json:"region"`
	AvailabilityZone string `json:"availabilityZone"`
}

func ResolveCloudMetadata(ctx context.Context, provider string) (CloudMetadata, error) {
	return resolveCloudMetadata(ctx, provider, cloudMetadataResolverOptions{})
}

func resolveCloudMetadata(ctx context.Context, provider string, options cloudMetadataResolverOptions) (CloudMetadata, error) {
	rawProvider := strings.TrimSpace(provider)
	normalizedProvider := strings.ToLower(rawProvider)
	metadata := CloudMetadata{CloudVendor: rawProvider}

	switch normalizedProvider {
	case "aws":
		resolved, err := resolveAWSCloudMetadata(ctx, options)
		resolved.CloudVendor = metadata.CloudVendor
		return resolved, err
	case "gcp":
		resolved, err := resolveGCPCloudMetadata(ctx, options)
		resolved.CloudVendor = metadata.CloudVendor
		return resolved, err
	default:
		return metadata, nil
	}
}

func resolveAWSCloudMetadata(ctx context.Context, options cloudMetadataResolverOptions) (CloudMetadata, error) {
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	baseURL := strings.TrimRight(strings.TrimSpace(options.AWSBaseURL), "/")
	if baseURL == "" {
		baseURL = awsMetadataBaseURL
	}

	tokenRequest, err := http.NewRequestWithContext(ctx, http.MethodPut, baseURL+awsIMDSTokenPath, nil)
	if err != nil {
		return CloudMetadata{}, err
	}
	tokenRequest.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", fmt.Sprintf("%d", awsMetadataTokenTTLSeconds))

	tokenResponse, err := client.Do(tokenRequest)
	if err != nil {
		return CloudMetadata{}, err
	}
	defer tokenResponse.Body.Close()
	if tokenResponse.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(tokenResponse.Body, 1024))
		return CloudMetadata{}, fmt.Errorf("aws imds token request returned %d: %s", tokenResponse.StatusCode, strings.TrimSpace(string(body)))
	}

	tokenBody, err := io.ReadAll(io.LimitReader(tokenResponse.Body, 4096))
	if err != nil {
		return CloudMetadata{}, err
	}
	token := strings.TrimSpace(string(tokenBody))
	if token == "" {
		return CloudMetadata{}, fmt.Errorf("aws imds returned an empty token")
	}

	documentRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+awsInstanceIdentityDocPath, nil)
	if err != nil {
		return CloudMetadata{}, err
	}
	documentRequest.Header.Set("X-aws-ec2-metadata-token", token)

	documentResponse, err := client.Do(documentRequest)
	if err != nil {
		return CloudMetadata{}, err
	}
	defer documentResponse.Body.Close()
	if documentResponse.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(documentResponse.Body, 1024))
		return CloudMetadata{}, fmt.Errorf("aws instance identity returned %d: %s", documentResponse.StatusCode, strings.TrimSpace(string(body)))
	}

	var document awsInstanceIdentityDocument
	if err := json.NewDecoder(io.LimitReader(documentResponse.Body, 16*1024)).Decode(&document); err != nil {
		return CloudMetadata{}, fmt.Errorf("decode aws instance identity document: %w", err)
	}

	return CloudMetadata{
		InstanceType:     strings.TrimSpace(document.InstanceType),
		Region:           strings.TrimSpace(document.Region),
		AvailabilityZone: strings.TrimSpace(document.AvailabilityZone),
	}, nil
}

func resolveGCPCloudMetadata(ctx context.Context, options cloudMetadataResolverOptions) (CloudMetadata, error) {
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	baseURL := strings.TrimRight(strings.TrimSpace(options.GCPBaseURL), "/")
	if baseURL == "" {
		baseURL = gcpMetadataBaseURL
	}

	machineType, err := readGCPMetadataValue(ctx, client, baseURL+gcpMachineTypePath)
	if err != nil {
		return CloudMetadata{}, err
	}
	zone, err := readGCPMetadataValue(ctx, client, baseURL+gcpZonePath)
	if err != nil {
		return CloudMetadata{InstanceType: lastPathSegment(machineType)}, err
	}

	availabilityZone := lastPathSegment(zone)
	return CloudMetadata{
		InstanceType:     lastPathSegment(machineType),
		Region:           regionFromZone(availabilityZone),
		AvailabilityZone: availabilityZone,
	}, nil
}

func readGCPMetadataValue(ctx context.Context, client *http.Client, url string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set(gcpMetadataFlavorHeader, "Google")

	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return "", fmt.Errorf("gcp metadata returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

func lastPathSegment(value string) string {
	trimmed := strings.Trim(strings.TrimSpace(value), "/")
	if trimmed == "" {
		return ""
	}
	parts := strings.Split(trimmed, "/")
	return parts[len(parts)-1]
}

func regionFromZone(zone string) string {
	trimmed := strings.TrimSpace(zone)
	lastDash := strings.LastIndex(trimmed, "-")
	if lastDash <= 0 {
		return ""
	}
	return trimmed[:lastDash]
}
