package siastorage

import (
	"bytes"
	"strings"
	"testing"
)

func TestObjectMetadata(t *testing.T) {
	obj := NewEmptyObject()
	if meta := obj.Metadata(); meta != nil {
		t.Fatalf("expected no metadata, got %q", meta)
	}
	want := []byte(`{"filename":"test.txt"}`)
	obj.UpdateMetadata(want)
	if got := obj.Metadata(); !bytes.Equal(got, want) {
		t.Fatalf("expected %q, got %q", want, got)
	}
	obj.UpdateMetadata(nil)
	if meta := obj.Metadata(); meta != nil {
		t.Fatalf("expected cleared metadata, got %q", meta)
	}
}

func TestNewSeedPhrase(t *testing.T) {
	phrase := NewSeedPhrase()
	if words := strings.Fields(phrase); len(words) != 12 {
		t.Fatalf("expected 12 words, got %d: %q", len(words), phrase)
	}
	if NewSeedPhrase() == phrase {
		t.Fatal("expected unique phrases")
	}
}

func TestGenerateAppID(t *testing.T) {
	id := GenerateAppID()
	if id == ([32]byte{}) {
		t.Fatal("expected non-zero app ID")
	}
	if GenerateAppID() == id {
		t.Fatal("expected unique app IDs")
	}
}
