package supabase

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// When the storage backend rejects an upload it responds with a non-2xx
// status and a JSON error body. Upload must surface that as an error
// instead of reporting success.
func TestUploadReturnsErrorOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"statusCode":"400","error":"InvalidRequest","message":"upload rejected"}`))
	}))
	defer srv.Close()

	client := CreateClient(srv.URL, "test-key")

	_, err := client.Storage.From("test-bucket").Upload(
		"folder/doc.json",
		strings.NewReader(`{"hello":"world"}`),
		nil,
	)
	if err == nil {
		t.Fatal("expected Upload to return an error on HTTP 400, got nil")
	}
	if !strings.Contains(err.Error(), "upload rejected") {
		t.Fatalf("expected error to carry the server message, got: %v", err)
	}
}

// Upload must send the content-type the caller asked for, rather than
// forcing a default. A bucket with allowed_mime_types will otherwise
// reject the upload with 415.
func TestUploadHonorsContentType(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("content-type")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Key":"test-bucket/folder/doc.json"}`))
	}))
	defer srv.Close()

	client := CreateClient(srv.URL, "test-key")
	_, err := client.Storage.From("test-bucket").Upload(
		"folder/doc.json",
		strings.NewReader(`{"hello":"world"}`),
		&FileUploadOptions{ContentType: "application/json"},
	)
	if err != nil {
		t.Fatalf("upload error: %v", err)
	}
	if got != "application/json" {
		t.Fatalf("content-type not honored: got %q want application/json", got)
	}
}

func TestUploadSucceedsOn2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Key":"test-bucket/folder/doc.json"}`))
	}))
	defer srv.Close()

	client := CreateClient(srv.URL, "test-key")

	resp, err := client.Storage.From("test-bucket").Upload(
		"folder/doc.json",
		strings.NewReader(`{"hello":"world"}`),
		nil,
	)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if resp.Key != "test-bucket/folder/doc.json" {
		t.Fatalf("unexpected key: %q", resp.Key)
	}
}
