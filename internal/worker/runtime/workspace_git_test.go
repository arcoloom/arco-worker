package runtime

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func TestMaterializeGitSource_ClonesDefaultBranch(t *testing.T) {
	t.Parallel()

	repoPath, history := createTestGitRepository(t)
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	err := materializeGitSource(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), workspaceRoot, &SourceSpec{
		Kind: "git",
		URI:  repoPath,
	})
	if err != nil {
		t.Fatalf("materialize git source: %v", err)
	}

	assertFileContent(t, filepath.Join(workspaceRoot, "app", "main.txt"), history.defaultBranchContent)
}

func TestMaterializeGitSource_CheckoutRemoteBranchByShortName(t *testing.T) {
	t.Parallel()

	repoPath, history := createTestGitRepository(t)
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	err := materializeGitSource(context.Background(), nil, workspaceRoot, &SourceSpec{
		Kind:     "git",
		URI:      repoPath,
		Revision: "feature",
	})
	if err != nil {
		t.Fatalf("materialize git source: %v", err)
	}

	assertFileContent(t, filepath.Join(workspaceRoot, "app", "main.txt"), history.featureBranchContent)
}

func TestMaterializeGitSource_CheckoutCommitHash(t *testing.T) {
	t.Parallel()

	repoPath, history := createTestGitRepository(t)
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	err := materializeGitSource(context.Background(), nil, workspaceRoot, &SourceSpec{
		Kind:     "git",
		URI:      repoPath,
		Revision: history.initialCommit.String(),
	})
	if err != nil {
		t.Fatalf("materialize git source: %v", err)
	}

	assertFileContent(t, filepath.Join(workspaceRoot, "app", "main.txt"), history.initialCommitContent)
}

type gitRepoHistory struct {
	initialCommit        plumbing.Hash
	initialCommitContent string
	defaultBranchContent string
	featureBranchContent string
}

func createTestGitRepository(t *testing.T) (string, gitRepoHistory) {
	t.Helper()

	repoPath := filepath.Join(t.TempDir(), "origin")
	repo, err := git.PlainInit(repoPath, false)
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}

	worktree, err := repo.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}

	initialCommit := createTestGitCommit(t, repoPath, worktree, "app/main.txt", "hello initial", "initial commit", time.Unix(1, 0))
	createTestGitCommit(t, repoPath, worktree, "app/main.txt", "hello master", "master commit", time.Unix(2, 0))

	if err := worktree.Checkout(&git.CheckoutOptions{
		Branch: plumbing.NewBranchReferenceName("feature"),
		Create: true,
		Hash:   initialCommit,
	}); err != nil {
		t.Fatalf("checkout feature: %v", err)
	}

	createTestGitCommit(t, repoPath, worktree, "app/main.txt", "hello feature", "feature commit", time.Unix(3, 0))

	if err := worktree.Checkout(&git.CheckoutOptions{
		Branch: plumbing.Master,
	}); err != nil {
		t.Fatalf("checkout master: %v", err)
	}

	return repoPath, gitRepoHistory{
		initialCommit:        initialCommit,
		initialCommitContent: "hello initial",
		defaultBranchContent: "hello master",
		featureBranchContent: "hello feature",
	}
}

func createTestGitCommit(
	t *testing.T,
	repoPath string,
	worktree *git.Worktree,
	relativePath string,
	content string,
	message string,
	when time.Time,
) plumbing.Hash {
	t.Helper()

	targetPath := filepath.Join(repoPath, relativePath)
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		t.Fatalf("mkdir commit parent: %v", err)
	}
	if err := os.WriteFile(targetPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write commit file: %v", err)
	}
	if _, err := worktree.Add(relativePath); err != nil {
		t.Fatalf("add commit file: %v", err)
	}

	hash, err := worktree.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Arco Worker Test",
			Email: "worker-test@example.com",
			When:  when,
		},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	return hash
}

func assertFileContent(t *testing.T, targetPath string, want string) {
	t.Helper()

	body, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read file %q: %v", targetPath, err)
	}
	if string(body) != want {
		t.Fatalf("file %q = %q, want %q", targetPath, string(body), want)
	}
}
