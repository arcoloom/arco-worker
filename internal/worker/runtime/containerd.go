package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type resolvedContainerMount struct {
	Source   string
	Target   string
	ReadOnly bool
}

func parseContainerPayload(ctx context.Context, raw []byte) (ContainerPayload, error) {
	if err := ctx.Err(); err != nil {
		return ContainerPayload{}, err
	}
	if len(raw) == 0 {
		return ContainerPayload{}, errors.New("container payload is empty")
	}

	var payload ContainerPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ContainerPayload{}, fmt.Errorf("decode container payload: %w", err)
	}

	if strings.TrimSpace(payload.TaskID) == "" {
		return ContainerPayload{}, errors.New("container payload task_id is required")
	}
	if strings.TrimSpace(payload.WorkspaceRoot) == "" {
		return ContainerPayload{}, errors.New("container payload workspace_root is required")
	}
	if strings.TrimSpace(payload.Image) == "" {
		return ContainerPayload{}, errors.New("container payload image is required")
	}
	if len(payload.Command) > 0 && strings.TrimSpace(payload.Command[0]) == "" {
		return ContainerPayload{}, errors.New("container payload command[0] must not be empty")
	}
	normalizedGPUDevices, err := normalizeContainerGPUDeviceRequests(payload.GPUDevices)
	if err != nil {
		return ContainerPayload{}, err
	}
	payload.GPUDevices = normalizedGPUDevices

	for index, mount := range payload.DirectoryMounts {
		if strings.TrimSpace(mount.Source) == "" {
			return ContainerPayload{}, fmt.Errorf("container payload directory_mounts[%d].source is required", index)
		}
		if strings.TrimSpace(mount.Target) == "" {
			return ContainerPayload{}, fmt.Errorf("container payload directory_mounts[%d].target is required", index)
		}
	}
	for index, mapping := range payload.Ports {
		if mapping.ContainerPort < 1 || mapping.ContainerPort > 65535 {
			return ContainerPayload{}, fmt.Errorf("container payload ports[%d].container_port must be between 1 and 65535", index)
		}
		hostPort := mapping.HostPort
		if hostPort == 0 {
			hostPort = mapping.ContainerPort
			payload.Ports[index].HostPort = hostPort
		}
		if hostPort < 1 || hostPort > 65535 {
			return ContainerPayload{}, fmt.Errorf("container payload ports[%d].host_port must be between 1 and 65535", index)
		}
		protocol := strings.ToLower(strings.TrimSpace(mapping.Protocol))
		if protocol == "" {
			protocol = "tcp"
		}
		switch protocol {
		case "tcp", "udp":
			payload.Ports[index].Protocol = protocol
		default:
			return ContainerPayload{}, fmt.Errorf("container payload ports[%d].protocol must be tcp or udp", index)
		}
	}

	return payload, nil
}

func normalizeContainerGPUDeviceRequests(devices []string) ([]string, error) {
	if len(devices) == 0 {
		return nil, nil
	}

	result := make([]string, 0, len(devices))
	seen := make(map[string]struct{}, len(devices))
	for index, raw := range devices {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return nil, fmt.Errorf("container payload gpu_devices[%d] must not be empty", index)
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result, nil
}

func resolveCDIDeviceRequests(vendor string, class string, available []string, requested []string, count int) ([]string, error) {
	normalizedAvailable, err := normalizeContainerGPUDeviceRequests(available)
	if err != nil {
		return nil, fmt.Errorf("normalize available CDI devices: %w", err)
	}

	availableSet := make(map[string]struct{}, len(normalizedAvailable))
	for _, name := range normalizedAvailable {
		availableSet[name] = struct{}{}
	}

	normalizedRequested, err := normalizeContainerGPUDeviceRequests(requested)
	if err != nil {
		return nil, err
	}
	if len(normalizedRequested) > 0 {
		qualified := make([]string, 0, len(normalizedRequested))
		for _, request := range normalizedRequested {
			name, err := normalizeCDIDeviceRequest(vendor, class, request)
			if err != nil {
				return nil, err
			}
			if _, exists := availableSet[name]; !exists {
				return nil, fmt.Errorf("requested gpu device %q is not available", name)
			}
			qualified = append(qualified, qualifyCDIDeviceName(vendor, class, name))
		}
		return qualified, nil
	}

	if count <= 0 {
		return nil, nil
	}

	selected, err := selectPreferredCDIDeviceNames(normalizedAvailable, count)
	if err != nil {
		return nil, err
	}

	qualified := make([]string, 0, len(selected))
	for _, name := range selected {
		qualified = append(qualified, qualifyCDIDeviceName(vendor, class, name))
	}
	return qualified, nil
}

func normalizeCDIDeviceRequest(vendor string, class string, request string) (string, error) {
	trimmed := strings.TrimSpace(request)
	if trimmed == "" {
		return "", errors.New("gpu device request must not be empty")
	}

	prefix := vendor + "/" + class + "="
	if strings.HasPrefix(trimmed, prefix) {
		return strings.TrimPrefix(trimmed, prefix), nil
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "=") {
		return "", fmt.Errorf("gpu device request %q must use %s<name> or be unqualified", trimmed, prefix)
	}
	return trimmed, nil
}

func qualifyCDIDeviceName(vendor string, class string, name string) string {
	return vendor + "/" + class + "=" + strings.TrimSpace(name)
}

func selectPreferredCDIDeviceNames(available []string, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}

	preferred := preferredCDIDeviceNames(available)
	if len(preferred) < count {
		return nil, fmt.Errorf("gpu_count=%d requested but only %d CDI device(s) are available", count, len(preferred))
	}

	selected := make([]string, 0, count)
	selected = append(selected, preferred[:count]...)
	return selected, nil
}

func preferredCDIDeviceNames(available []string) []string {
	var indexStyle []string
	var fallback []string
	for _, name := range available {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" || trimmed == "all" {
			continue
		}
		if isIndexStyleCDIDeviceName(trimmed) {
			indexStyle = append(indexStyle, trimmed)
			continue
		}
		fallback = append(fallback, trimmed)
	}
	if len(indexStyle) > 0 {
		return indexStyle
	}
	return fallback
}

func isIndexStyleCDIDeviceName(name string) bool {
	if name == "" {
		return false
	}

	parts := strings.Split(name, ":")
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

func containerTaskName(taskID string) string {
	base := "arco-task-" + strings.ToLower(strings.TrimSpace(taskID))
	var builder strings.Builder
	builder.Grow(len(base))

	lastDash := false
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		case r == '-', r == '_', r == '.', r == '/':
			if builder.Len() == 0 || lastDash {
				continue
			}
			builder.WriteByte('-')
			lastDash = true
		}
	}

	name := strings.Trim(builder.String(), "-")
	if name == "" {
		return "arco-task"
	}
	if len(name) > 120 {
		return strings.TrimRight(name[:120], "-")
	}
	return name
}

func resolveContainerDirectoryMounts(ctx context.Context, workspaceRoot string, mounts []ContainerMount) ([]resolvedContainerMount, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(mounts) == 0 {
		return []resolvedContainerMount{}, nil
	}

	result := make([]resolvedContainerMount, 0, len(mounts))
	for index, mount := range mounts {
		source, err := resolveContainerMountSource(workspaceRoot, mount.Source)
		if err != nil {
			return nil, fmt.Errorf("resolve directory mount %d source: %w", index, err)
		}
		if err := ensureContainerMountSource(source, mount.Source, workspaceRoot); err != nil {
			return nil, fmt.Errorf("prepare directory mount %d source: %w", index, err)
		}
		target := filepath.Clean(strings.TrimSpace(mount.Target))
		if !strings.HasPrefix(target, "/") || target == "/" {
			return nil, fmt.Errorf("directory mount %d target must be an absolute path other than /", index)
		}
		result = append(result, resolvedContainerMount{
			Source:   source,
			Target:   target,
			ReadOnly: mount.ReadOnly,
		})
	}
	return result, nil
}

func resolveContainerMountSource(workspaceRoot string, source string) (string, error) {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return "", errors.New("source is required")
	}
	if filepath.IsAbs(trimmed) {
		return filepath.Clean(trimmed), nil
	}
	if strings.HasPrefix(trimmed, ".."+string(os.PathSeparator)) || trimmed == ".." {
		return "", errors.New("relative source must stay within the task workspace")
	}
	return filepath.Join(filepath.Clean(workspaceRoot), filepath.Clean(trimmed)), nil
}

func ensureContainerMountSource(resolvedSource string, originalSource string, workspaceRoot string) error {
	info, err := os.Stat(resolvedSource)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("source %q is not a directory", originalSource)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if filepath.IsAbs(strings.TrimSpace(originalSource)) {
		return err
	}

	workspace := filepath.Clean(workspaceRoot)
	if !strings.HasPrefix(resolvedSource, workspace+string(os.PathSeparator)) && resolvedSource != workspace {
		return fmt.Errorf("relative source %q escapes the task workspace", originalSource)
	}
	if createErr := os.MkdirAll(resolvedSource, 0o755); createErr != nil {
		return createErr
	}
	return nil
}
