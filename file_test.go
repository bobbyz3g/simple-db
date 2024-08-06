package simpledb

import (
	"os"
	"path"
	"testing"
)

func TestSaveData(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "temp")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %s", err)
	}

	// 确保测试结束后删除临时目录
	defer os.RemoveAll(tempDir)

	name := path.Join(tempDir, "data")

	if err := SaveData(name, []byte("data1")); err != nil {
		t.Fatalf("SaveData failed: %v", err)
	}

	b, _ := os.ReadFile(name)
	if got := string(b); got != "data1" {
		t.Fatalf("Want data1, got %q", got)
	}
}
