package supabase

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Probe what the server actually receives for an Upload, to diagnose
// why an object can be reported "uploaded" yet never appear in storage.
func TestUploadRequestShape(t *testing.T) {
	var (
		gotCL    int64
		gotTE    []string
		gotBody  string
		gotCType string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCL = r.ContentLength
		gotTE = r.TransferEncoding
		gotCType = r.Header.Get("content-type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"Key":"test-bucket/folder/doc.json"}`))
	}))
	defer srv.Close()

	client := CreateClient(srv.URL, "test-key")
	payload := `{"hello":"world","n":1}`
	_, err := client.Storage.From("test-bucket").Upload("folder/doc.json", strings.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("upload error: %v", err)
	}

	t.Logf("ContentLength=%d TransferEncoding=%v content-type=%q bodyLen=%d", gotCL, gotTE, gotCType, len(gotBody))
	if gotBody != payload {
		t.Errorf("body mismatch: got %q want %q", gotBody, payload)
	}
	if gotCL <= 0 {
		t.Errorf("Content-Length not set (got %d) -> request is chunked; some storage backends reject this", gotCL)
	}
	for _, te := range gotTE {
		if te == "chunked" {
			t.Errorf("request sent chunked; some storage backends reject chunked uploads")
		}
	}
}
