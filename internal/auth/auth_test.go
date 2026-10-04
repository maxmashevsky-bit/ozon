package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadKeyFileAndErrors(t *testing.T) {
	t.Setenv("JWT_SECRET", strings.Repeat("e", 32))
	key, err := ReadKey("")
	if err != nil || string(key) != strings.Repeat("e", 32) {
		t.Fatalf("env key: %v", err)
	}
	path := filepath.Join(t.TempDir(), "jwt.key")
	if err := os.WriteFile(path, []byte(strings.Repeat("f", 32)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	key, err = ReadKey(path)
	if err != nil || string(key) != strings.Repeat("f", 32) {
		t.Fatalf("file key: %v", err)
	}
	if _, err = ReadKey(path + "-missing"); err == nil || !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("missing file: %v", err)
	}
	if err := os.WriteFile(path, []byte("short"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadKey(path); err == nil || !strings.Contains(err.Error(), "at least 32") {
		t.Fatalf("short file: %v", err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadKey(path); err == nil || !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("unreadable file: %v", err)
	}
}
