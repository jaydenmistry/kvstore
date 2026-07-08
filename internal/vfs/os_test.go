package vfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOSRoundTrip(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "f")
	f, err := OS{}.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := (OS{}).SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(4); err != nil {
		t.Fatal(err)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 4 {
		t.Fatalf("size = %d, want 4", size)
	}
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hell" {
		t.Fatalf("read %q, want %q", buf, "hell")
	}
}

func TestOSRenameAndRemove(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	f, err := OS{}.OpenFile(a, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := (OS{}).Rename(a, b); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("old name still exists: %v", err)
	}
	if err := (OS{}).Remove(b); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(b); !os.IsNotExist(err) {
		t.Fatalf("removed file still exists: %v", err)
	}
}
