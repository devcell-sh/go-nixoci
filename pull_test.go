package nixoci_test

// Pull tests — RED phase of TDD for CELL-293.
//
// What we're testing:
//
//  1. Pull(srcRef, dstDir) downloads an OCI image's LAST layer and
//     extracts it (as a gzipped tarball) into dstDir. dstDir is the
//     filesystem root onto which `nix/...` archive paths land directly
//     (no --strip-components needed inside the package — the workflow
//     extracts into a mount where /dest IS the volume root, so archive
//     entries like `nix/store/...` become /dest/nix/store/...).
//
//  2. The pulled bytes match the input bytes — i.e., what we push and
//     what we pull are byte-identical. This is the exact assertion
//     CELL-292 burned 30 commits failing to maintain.
//
// Setup: an in-memory OCI registry (httptest + pkg/registry from
// go-containerregistry, already imported by internal/runner/registry.go).
// We seed it with a known image (busybox base + a known tar.gz layer
// containing a fixture nix-store layout) and exercise Pull against it.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	nixoci "github.com/devcell-sh/go-nixoci"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// TestPull_RoundTripsFixture seeds an in-memory registry with an image
// whose last layer contains a known nix-store-shaped tar.gz, calls
// nixoci.Pull, and asserts every file under dstDir matches the fixture
// byte-for-byte.
func TestPull_RoundTripsFixture(t *testing.T) {
	srv := newRegistry(t)
	defer srv.Close()

	regHost := mustHost(t, srv.URL)
	imgRef := regHost + "/nix-store:test"

	// Fixture: a few nix-store-shaped paths. Keep it small so tests are fast.
	fixture := map[string][]byte{
		"nix/store/aaa-hello/bin/hello":       []byte("#!/bin/sh\necho hello\n"),
		"nix/store/bbb-world/bin/world":       []byte("#!/bin/sh\necho world\n"),
		"nix/var/log/nix/drvs/sample.drv.log": []byte("build log line\n"),
		"nix/.fixture-marker":                 []byte("marker"),
	}
	layerBytes := mustBuildTarGz(t, fixture)

	// Push: base image + one layer containing the fixture tar.gz.
	mustPushImage(t, imgRef, layerBytes)

	// RED: nixoci.Pull doesn't exist yet. Once it does:
	//   - it should fetch the manifest
	//   - download the LAST layer
	//   - extract it into dstDir
	dstDir := t.TempDir()
	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("nixoci.Pull(%q, %q) failed: %v", imgRef, dstDir, err)
	}

	// Verify byte-for-byte content match.
	for relPath, want := range fixture {
		got, err := os.ReadFile(filepath.Join(dstDir, relPath))
		if err != nil {
			t.Errorf("expected %s under dstDir, got error: %v", relPath, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s content mismatch:\nwant %q\ngot  %q", relPath, want, got)
		}
	}
}

// TestPull_RejectsNonexistentImage ensures Pull returns an error (not a
// panic, not a silent success) when the source image doesn't exist.
func TestPull_RejectsNonexistentImage(t *testing.T) {
	srv := newRegistry(t)
	defer srv.Close()

	imgRef := mustHost(t, srv.URL) + "/no-such:image"
	dstDir := t.TempDir()
	err := nixoci.Pull(context.Background(), imgRef, dstDir, 0)
	if err == nil {
		t.Fatal("expected Pull to fail for nonexistent image, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "manifest") &&
		!strings.Contains(strings.ToLower(err.Error()), "not found") &&
		!strings.Contains(strings.ToLower(err.Error()), "unknown") {
		t.Errorf("error should reference manifest/not-found/unknown; got: %v", err)
	}
}

// TestPull_LastLayerOnly verifies Pull extracts only the LAST layer of a
// multi-layer image (the workflow's contract — the nix-store layer is
// always atop a busybox base). Earlier layers' content must not appear
// under dstDir.
func TestPull_LastLayerOnly(t *testing.T) {
	srv := newRegistry(t)
	defer srv.Close()

	regHost := mustHost(t, srv.URL)
	imgRef := regHost + "/nix-store:multi"

	baseFixture := map[string][]byte{"unwanted-base/file": []byte("BASE LAYER")}
	topFixture := map[string][]byte{"nix/store/zzz/marker": []byte("TOP LAYER")}

	mustPushImageWithLayers(t, imgRef, mustBuildTarGz(t, baseFixture), mustBuildTarGz(t, topFixture))

	dstDir := t.TempDir()
	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("Pull failed: %v", err)
	}

	// Top layer's file should exist.
	if _, err := os.Stat(filepath.Join(dstDir, "nix/store/zzz/marker")); err != nil {
		t.Errorf("expected top-layer file extracted; got: %v", err)
	}
	// Base layer's file must NOT exist.
	if _, err := os.Stat(filepath.Join(dstDir, "unwanted-base/file")); !os.IsNotExist(err) {
		t.Errorf("base-layer file should not be extracted; stat err = %v", err)
	}
}

// ── test helpers ──────────────────────────────────────────────────────

// newRegistry spins up the in-memory go-containerregistry server on a
// random port. Tests are isolated by per-test temp dir for the blob store.
func newRegistry(t *testing.T) *httptest.Server {
	t.Helper()
	srv, _ := newRegistryWithBlobs(t)
	return srv
}

// newRegistryWithBlobs is newRegistry plus the on-disk blob directory,
// so tests can corrupt or replace individual layer blobs. A blob with
// digest sha256:HEX lives at <dir>/sha256/HEX.
func newRegistryWithBlobs(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	blobDir := t.TempDir()
	handler := registry.New(
		registry.WithBlobHandler(registry.NewDiskBlobHandler(blobDir)),
	)
	return httptest.NewServer(handler), blobDir
}

func blobPath(blobDir string, layerGz []byte) string {
	return filepath.Join(blobDir, "sha256", sha256Hex(layerGz))
}

func markerPath(dstDir string, layerGz []byte) string {
	return filepath.Join(dstDir, nixoci.MarkerDir, sha256Hex(layerGz))
}

func assertExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Lstat(path)
	switch {
	case want && err != nil:
		t.Errorf("expected %s to exist: %v", path, err)
	case !want && !os.IsNotExist(err):
		t.Errorf("expected %s to be absent; stat err = %v", path, err)
	}
}

func mustImageDigest(t *testing.T, imgRef string) string {
	t.Helper()
	ref, err := name.ParseReference(imgRef)
	if err != nil {
		t.Fatalf("parse ref: %v", err)
	}
	img, err := remote.Image(ref)
	if err != nil {
		t.Fatalf("remote.Image: %v", err)
	}
	d, err := img.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return d.String()
}

func mustCopyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// tarEntry describes one archive entry for mustBuildTarGzEntries; Body
// is ignored for non-regular entries.
type tarEntry struct {
	Header *tar.Header
	Body   []byte
}

func mustBuildTarGzEntries(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		if e.Header.Typeflag == tar.TypeReg {
			e.Header.Size = int64(len(e.Body))
		}
		if err := tw.WriteHeader(e.Header); err != nil {
			t.Fatalf("tar header %s: %v", e.Header.Name, err)
		}
		if e.Header.Typeflag == tar.TypeReg {
			if _, err := tw.Write(e.Body); err != nil {
				t.Fatalf("tar write %s: %v", e.Header.Name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// wrongDiffIDLayer reports a DiffID that does not match its content, so
// the pushed image's config carries a bogus diff_id for this layer.
type wrongDiffIDLayer struct{ v1.Layer }

func (wrongDiffIDLayer) DiffID() (v1.Hash, error) {
	return v1.NewHash("sha256:" + strings.Repeat("ab", 32))
}

func mustPushImageWithV1Layers(t *testing.T, imgRef string, layers ...v1.Layer) {
	t.Helper()
	img := empty.Image
	for _, l := range layers {
		var err error
		img, err = mutate.AppendLayers(img, l)
		if err != nil {
			t.Fatalf("AppendLayers: %v", err)
		}
	}
	ref, err := name.ParseReference(imgRef)
	if err != nil {
		t.Fatalf("parse ref %s: %v", imgRef, err)
	}
	if err := remote.Write(ref, img, remote.WithTransport(http.DefaultTransport)); err != nil {
		t.Fatalf("remote.Write: %v", err)
	}
}

func mustTarballLayer(t *testing.T, gz []byte) v1.Layer {
	t.Helper()
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(gz)), nil
	})
	if err != nil {
		t.Fatalf("tarball.LayerFromOpener: %v", err)
	}
	return l
}

func requireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not available")
	}
	if err := exec.Command("docker", "pull", "-q", "public.ecr.aws/docker/library/alpine:latest").Run(); err != nil {
		t.Skip("cannot pull alpine image (no network?)")
	}
}

func mustDockerVolume(t *testing.T) string {
	t.Helper()
	vol := "nixoci-test-" + sha256Hex([]byte(t.Name()))[:12]
	if out, err := exec.Command("docker", "volume", "create", vol).CombinedOutput(); err != nil {
		t.Fatalf("docker volume create: %v: %s", err, out)
	}
	t.Cleanup(func() { exec.Command("docker", "volume", "rm", "-f", vol).Run() })
	return vol
}

func dockerVolumeLs(t *testing.T, vol, dir string) []string {
	t.Helper()
	out, err := exec.Command("docker", "run", "--rm", "-v", vol+":/dest",
		"public.ecr.aws/docker/library/alpine:latest", "sh", "-c", "ls -1 /dest/"+dir+" 2>/dev/null").Output()
	if err != nil {
		t.Fatalf("ls %s on %s: %v", dir, vol, err)
	}
	return strings.Fields(string(out))
}

func dockerVolumeCat(t *testing.T, vol, path string) string {
	t.Helper()
	out, err := exec.Command("docker", "run", "--rm", "-v", vol+":/dest",
		"public.ecr.aws/docker/library/alpine:latest", "cat", "/dest/"+path).Output()
	if err != nil {
		t.Fatalf("cat %s on %s: %v", path, vol, err)
	}
	return string(out)
}

// mustHost strips http:// and returns host:port for crane-style refs.
func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	return u.Host
}

// mustBuildTarGz builds a gzipped tarball from the given path → content
// map. Used to construct fixture layers.
func mustBuildTarGz(t *testing.T, contents map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for path, data := range contents {
		hdr := &tar.Header{
			Name: path,
			Mode: 0644,
			Size: int64(len(data)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar header %s: %v", path, err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatalf("tar write %s: %v", path, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// mustPushImage publishes a single-layer image to the test registry with
// the given gzipped-tarball layer bytes. The layer's media type is OCI
// tar+gzip (matches what the production workflow produces).
func mustPushImage(t *testing.T, imgRef string, layerGz []byte) {
	t.Helper()
	mustPushImageWithLayers(t, imgRef, layerGz)
}

// mustPushImageWithLayers builds an image with N layers (in order — last
// arg is the topmost layer) and pushes it to the registry. Used by
// TestPull_LastLayerOnly to construct a multi-layer fixture.
func mustPushImageWithLayers(t *testing.T, imgRef string, layersGz ...[]byte) {
	t.Helper()
	img := empty.Image
	for _, gz := range layersGz {
		l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(gz)), nil
		})
		if err != nil {
			t.Fatalf("tarball.LayerFromOpener: %v", err)
		}
		img, err = mutate.AppendLayers(img, l)
		if err != nil {
			t.Fatalf("AppendLayers: %v", err)
		}
	}
	ref, err := name.ParseReference(imgRef)
	if err != nil {
		t.Fatalf("parse ref %s: %v", imgRef, err)
	}
	if err := remote.Write(ref, img, remote.WithTransport(http.DefaultTransport)); err != nil {
		t.Fatalf("remote.Write: %v", err)
	}
}

// sha256Hex is a small helper for assertions on byte content (unused for
// now but kept handy for future tests that compare layer digests).
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestPull_MultiLayer verifies Pull extracts ALL non-base layers (not just
// the last one). This is the pull-side contract for CELL-297's chunked push:
// an image has 1 base layer + N data layers, and Pull must extract all N.
func TestPull_MultiLayer(t *testing.T) {
	srv := newRegistry(t)
	defer srv.Close()

	regHost := mustHost(t, srv.URL)
	imgRef := regHost + "/nix-store:multi-data"

	baseFixture := map[string][]byte{"base/marker": []byte("BASE")}
	dataLayer1 := map[string][]byte{
		"nix/store/aaa/bin/tool1": []byte("tool1-content"),
	}
	dataLayer2 := map[string][]byte{
		"nix/store/bbb/bin/tool2": []byte("tool2-content"),
	}

	mustPushImageWithLayers(t, imgRef,
		mustBuildTarGz(t, baseFixture),
		mustBuildTarGz(t, dataLayer1),
		mustBuildTarGz(t, dataLayer2),
	)

	dstDir := t.TempDir()
	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("Pull failed: %v", err)
	}

	// Both data layers' files must be extracted.
	for _, tc := range []struct{ path, want string }{
		{"nix/store/aaa/bin/tool1", "tool1-content"},
		{"nix/store/bbb/bin/tool2", "tool2-content"},
	} {
		got, err := os.ReadFile(filepath.Join(dstDir, tc.path))
		if err != nil {
			t.Errorf("expected %s extracted; got: %v", tc.path, err)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("%s = %q, want %q", tc.path, got, tc.want)
		}
	}

	// Base layer content must NOT be extracted.
	if _, err := os.Stat(filepath.Join(dstDir, "base/marker")); !os.IsNotExist(err) {
		t.Errorf("base layer content should not be extracted; stat err = %v", err)
	}
}

// TestPull_SingleLayer_BackwardCompat verifies old-style images (base + 1
// data layer) still pull correctly after the multi-layer extraction change.
func TestPull_SingleLayer_BackwardCompat(t *testing.T) {
	srv := newRegistry(t)
	defer srv.Close()

	regHost := mustHost(t, srv.URL)
	imgRef := regHost + "/nix-store:compat"

	baseFixture := map[string][]byte{"base/marker": []byte("BASE")}
	dataFixture := map[string][]byte{
		"nix/store/aaa/bin/tool": []byte("tool-content"),
		"nix/var/log/build.log":  []byte("log-content"),
	}

	mustPushImageWithLayers(t, imgRef,
		mustBuildTarGz(t, baseFixture),
		mustBuildTarGz(t, dataFixture),
	)

	dstDir := t.TempDir()
	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("Pull failed: %v", err)
	}

	for _, tc := range []struct{ path, want string }{
		{"nix/store/aaa/bin/tool", "tool-content"},
		{"nix/var/log/build.log", "log-content"},
	} {
		got, err := os.ReadFile(filepath.Join(dstDir, tc.path))
		if err != nil {
			t.Errorf("expected %s extracted; got: %v", tc.path, err)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("%s = %q, want %q", tc.path, got, tc.want)
		}
	}

	if _, err := os.Stat(filepath.Join(dstDir, "base/marker")); !os.IsNotExist(err) {
		t.Errorf("base layer content should not be extracted; stat err = %v", err)
	}
}

// ── per-layer commit markers ──────────────────────────────────────────

func TestPull_WritesMarkersAndImageDigest(t *testing.T) {
	srv := newRegistry(t)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:markers"

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data1 := mustBuildTarGz(t, map[string][]byte{"nix/store/aaa/f": []byte("1")})
	data2 := mustBuildTarGz(t, map[string][]byte{"nix/store/bbb/f": []byte("2")})
	mustPushImageWithLayers(t, imgRef, base, data1, data2)

	dstDir := t.TempDir()
	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("Pull failed: %v", err)
	}

	assertExists(t, markerPath(dstDir, base), false)
	assertExists(t, markerPath(dstDir, data1), true)
	assertExists(t, markerPath(dstDir, data2), true)

	got, err := os.ReadFile(filepath.Join(dstDir, nixoci.ImageMarker))
	if err != nil {
		t.Fatalf("read image marker: %v", err)
	}
	if want := mustImageDigest(t, imgRef); string(got) != want {
		t.Errorf("image marker = %q, want %q", got, want)
	}
}

func TestPull_FailedLayerLeavesNoMarker(t *testing.T) {
	setRetry(t, 1)
	srv, blobDir := newRegistryWithBlobs(t)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:failing"

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data1 := mustBuildTarGz(t, map[string][]byte{"nix/store/aaa/f": []byte("1")})
	data2 := mustBuildTarGz(t, map[string][]byte{"nix/store/bbb/f": []byte("2")})
	data3 := mustBuildTarGz(t, map[string][]byte{"nix/store/ccc/f": []byte("3")})
	mustPushImageWithLayers(t, imgRef, base, data1, data2, data3)

	// Corrupt layer 3 so its digest check fails mid-pull.
	if err := os.WriteFile(blobPath(blobDir, data3), []byte("garbage"), 0o644); err != nil {
		t.Fatalf("corrupt blob: %v", err)
	}

	dstDir := t.TempDir()
	err := nixoci.Pull(context.Background(), imgRef, dstDir, 0)
	if err == nil {
		t.Fatal("expected Pull to fail on corrupted layer 3")
	}
	if !strings.Contains(err.Error(), "layer 3") {
		t.Errorf("error should name layer 3; got: %v", err)
	}

	assertExists(t, markerPath(dstDir, data1), true)
	assertExists(t, markerPath(dstDir, data2), true)
	assertExists(t, markerPath(dstDir, data3), false)
	assertExists(t, filepath.Join(dstDir, nixoci.ImageMarker), false)
}

func TestPull_ResumesSkippingAppliedLayers(t *testing.T) {
	setRetry(t, 1)
	srv, blobDir := newRegistryWithBlobs(t)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:resume"

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data1 := mustBuildTarGz(t, map[string][]byte{"nix/store/aaa/f": []byte("1")})
	data2 := mustBuildTarGz(t, map[string][]byte{"nix/store/bbb/f": []byte("2")})
	data3 := mustBuildTarGz(t, map[string][]byte{"nix/store/ccc/f": []byte("3")})
	mustPushImageWithLayers(t, imgRef, base, data1, data2, data3)

	backup := filepath.Join(t.TempDir(), "data3.blob")
	mustCopyFile(t, blobPath(blobDir, data3), backup)
	if err := os.WriteFile(blobPath(blobDir, data3), []byte("garbage"), 0o644); err != nil {
		t.Fatalf("corrupt blob: %v", err)
	}

	dstDir := t.TempDir()
	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err == nil {
		t.Fatal("expected first Pull to fail")
	}

	// Restore layer 3, then poison layer 1: the resumed pull must not
	// read it, proving the marker short-circuits already applied layers.
	mustCopyFile(t, backup, blobPath(blobDir, data3))
	if err := os.WriteFile(blobPath(blobDir, data1), []byte("garbage"), 0o644); err != nil {
		t.Fatalf("poison blob: %v", err)
	}

	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("resumed Pull failed: %v", err)
	}

	for _, tc := range []struct{ path, want string }{
		{"nix/store/aaa/f", "1"},
		{"nix/store/bbb/f", "2"},
		{"nix/store/ccc/f", "3"},
	} {
		got, err := os.ReadFile(filepath.Join(dstDir, tc.path))
		if err != nil {
			t.Errorf("expected %s extracted; got: %v", tc.path, err)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("%s = %q, want %q", tc.path, got, tc.want)
		}
	}
	for _, gz := range [][]byte{data1, data2, data3} {
		assertExists(t, markerPath(dstDir, gz), true)
	}
	got, err := os.ReadFile(filepath.Join(dstDir, nixoci.ImageMarker))
	if err != nil {
		t.Fatalf("read image marker: %v", err)
	}
	if want := mustImageDigest(t, imgRef); string(got) != want {
		t.Errorf("image marker = %q, want %q", got, want)
	}
}

func TestPull_RetryOverwritesPartialSymlinks(t *testing.T) {
	srv := newRegistry(t)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:links"

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data := mustBuildTarGzEntries(t, []tarEntry{
		{Header: &tar.Header{Name: "nix/store/aaa/bin/tool", Mode: 0o755, Typeflag: tar.TypeReg}, Body: []byte("tool")},
		{Header: &tar.Header{Name: "nix/store/aaa/bin/alias", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: "tool"}},
		{Header: &tar.Header{Name: "nix/store/aaa/bin/hard", Mode: 0o755, Typeflag: tar.TypeLink, Linkname: "nix/store/aaa/bin/tool"}},
	})
	mustPushImageWithLayers(t, imgRef, base, data)

	// Simulate a previous partial extraction: both link entries already
	// exist, the symlink pointing somewhere wrong.
	dstDir := t.TempDir()
	binDir := filepath.Join(dstDir, "nix/store/aaa/bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("wrong-target", filepath.Join(binDir, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "hard"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("Pull failed: %v", err)
	}

	link, err := os.Readlink(filepath.Join(binDir, "alias"))
	if err != nil {
		t.Fatalf("readlink alias: %v", err)
	}
	if link != "tool" {
		t.Errorf("alias -> %q, want %q", link, "tool")
	}
	hard, err := os.ReadFile(filepath.Join(binDir, "hard"))
	if err != nil {
		t.Fatalf("read hard: %v", err)
	}
	if string(hard) != "tool" {
		t.Errorf("hard = %q, want %q", hard, "tool")
	}
	assertExists(t, markerPath(dstDir, data), true)
}

func TestPull_DiffIDMismatchRejected(t *testing.T) {
	setRetry(t, 1)
	srv := newRegistry(t)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:bad-diffid"

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data := mustBuildTarGz(t, map[string][]byte{"nix/store/aaa/f": []byte("1")})
	mustPushImageWithV1Layers(t, imgRef,
		mustTarballLayer(t, base),
		wrongDiffIDLayer{mustTarballLayer(t, data)},
	)

	dstDir := t.TempDir()
	err := nixoci.Pull(context.Background(), imgRef, dstDir, 0)
	if err == nil {
		t.Fatal("expected Pull to reject layer with mismatched DiffID")
	}
	if !strings.Contains(err.Error(), "diff id mismatch") {
		t.Errorf("error should mention diff id mismatch; got: %v", err)
	}
	assertExists(t, markerPath(dstDir, data), false)
	assertExists(t, filepath.Join(dstDir, nixoci.ImageMarker), false)
}

func TestPullToDockerVolume_WritesMarkers(t *testing.T) {
	requireDocker(t)
	srv := newRegistry(t)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:volume"

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data1 := mustBuildTarGz(t, map[string][]byte{"nix/store/aaa/f": []byte("1")})
	data2 := mustBuildTarGz(t, map[string][]byte{"nix/store/bbb/f": []byte("2")})
	mustPushImageWithLayers(t, imgRef, base, data1, data2)

	vol := mustDockerVolume(t)
	if err := nixoci.PullToDockerVolume(context.Background(), imgRef, vol, 0); err != nil {
		t.Fatalf("PullToDockerVolume failed: %v", err)
	}

	markers := dockerVolumeLs(t, vol, nixoci.MarkerDir)
	want := map[string]bool{sha256Hex(data1): true, sha256Hex(data2): true}
	if len(markers) != len(want) {
		t.Errorf("markers = %v, want exactly %v", markers, want)
	}
	for _, m := range markers {
		if !want[m] {
			t.Errorf("unexpected marker %s", m)
		}
	}
	if got, want := dockerVolumeCat(t, vol, nixoci.ImageMarker), mustImageDigest(t, imgRef); got != want {
		t.Errorf("image marker = %q, want %q", got, want)
	}
	if got := dockerVolumeCat(t, vol, "nix/store/bbb/f"); got != "2" {
		t.Errorf("nix/store/bbb/f = %q, want %q", got, "2")
	}

	// Second call must skip both layers and still succeed.
	if err := nixoci.PullToDockerVolume(context.Background(), imgRef, vol, 0); err != nil {
		t.Fatalf("second PullToDockerVolume failed: %v", err)
	}
}

// setRetry makes retries instant and caps attempts for the test's
// lifetime.
func setRetry(t *testing.T, attempts int) {
	t.Helper()
	prevDelay, prevAttempts := nixoci.RetryBaseDelay, nixoci.MaxDownloadAttempts
	nixoci.RetryBaseDelay = time.Millisecond
	nixoci.MaxDownloadAttempts = attempts
	t.Cleanup(func() {
		nixoci.RetryBaseDelay = prevDelay
		nixoci.MaxDownloadAttempts = prevAttempts
	})
}

// captureProgress routes nixoci.ProgressWriter into a buffer for the
// test's lifetime.
func captureProgress(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := nixoci.ProgressWriter
	buf := &bytes.Buffer{}
	nixoci.ProgressWriter = buf
	t.Cleanup(func() { nixoci.ProgressWriter = prev })
	return buf
}

// flakyBlobRegistry wraps the in-memory registry so GETs of one blob
// (by digest hex) fail with a truncated body for the first failUntil
// requests, then succeed. gets counts GET requests for that blob.
type flakyBlobRegistry struct {
	inner     http.Handler
	hex       string
	failUntil int
	gets      atomic.Int32
}

func (f *flakyBlobRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/blobs/sha256:"+f.hex) {
		n := int(f.gets.Add(1))
		if n <= f.failUntil {
			// Promise the full body, deliver a fragment: the client
			// sees a dropped connection, as with a flaky registry.
			w.Header().Set("Content-Length", "1000000")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("partial"))
			return
		}
	}
	f.inner.ServeHTTP(w, r)
}

func newFlakyRegistry(t *testing.T, layerGz []byte, failUntil int) (*httptest.Server, *flakyBlobRegistry) {
	t.Helper()
	inner := registry.New(
		registry.WithBlobHandler(registry.NewDiskBlobHandler(t.TempDir())),
	)
	flaky := &flakyBlobRegistry{inner: inner, hex: sha256Hex(layerGz), failUntil: failUntil}
	return httptest.NewServer(flaky), flaky
}

// TestPull_RetriesFailedLayer: a layer whose first download drops is
// re-streamed and the pull completes, as with docker pull's per-layer
// retry.
func TestPull_RetriesFailedLayer(t *testing.T) {
	setRetry(t, 5)

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data1 := mustBuildTarGz(t, map[string][]byte{"nix/store/aaa/f": []byte("1")})
	data2 := mustBuildTarGz(t, map[string][]byte{"nix/store/bbb/f": []byte("2")})

	srv, flaky := newFlakyRegistry(t, data2, 1)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:flaky"
	mustPushImageWithLayers(t, imgRef, base, data1, data2)

	dstDir := t.TempDir()
	if err := nixoci.Pull(context.Background(), imgRef, dstDir, 0); err != nil {
		t.Fatalf("Pull should succeed after retry; got: %v", err)
	}

	for _, tc := range []struct{ path, want string }{
		{"nix/store/aaa/f", "1"},
		{"nix/store/bbb/f", "2"},
	} {
		got, err := os.ReadFile(filepath.Join(dstDir, tc.path))
		if err != nil {
			t.Errorf("expected %s extracted; got: %v", tc.path, err)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("%s = %q, want %q", tc.path, got, tc.want)
		}
	}
	assertExists(t, markerPath(dstDir, data1), true)
	assertExists(t, markerPath(dstDir, data2), true)
	assertExists(t, filepath.Join(dstDir, nixoci.ImageMarker), true)
	if got := flaky.gets.Load(); got < 2 {
		t.Errorf("layer 2 blob GET count = %d, want >= 2 (a retry)", got)
	}
}

// TestPull_GivesUpAfterMaxAttempts: a persistently failing layer is
// attempted exactly MaxDownloadAttempts times, then the pull fails
// with earlier layers committed and no image marker.
func TestPull_GivesUpAfterMaxAttempts(t *testing.T) {
	setRetry(t, 2)

	base := mustBuildTarGz(t, map[string][]byte{"base/marker": []byte("BASE")})
	data1 := mustBuildTarGz(t, map[string][]byte{"nix/store/aaa/f": []byte("1")})
	data2 := mustBuildTarGz(t, map[string][]byte{"nix/store/bbb/f": []byte("2")})

	srv, flaky := newFlakyRegistry(t, data2, 1<<30)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/nix-store:dead"
	mustPushImageWithLayers(t, imgRef, base, data1, data2)

	dstDir := t.TempDir()
	err := nixoci.Pull(context.Background(), imgRef, dstDir, 0)
	if err == nil {
		t.Fatal("expected Pull to fail after exhausting attempts")
	}
	if !strings.Contains(err.Error(), "layer 2") {
		t.Errorf("error should name layer 2; got: %v", err)
	}
	if got := flaky.gets.Load(); got != 2 {
		t.Errorf("layer 2 blob GET count = %d, want exactly 2", got)
	}
	assertExists(t, markerPath(dstDir, data1), true)
	assertExists(t, markerPath(dstDir, data2), false)
	assertExists(t, filepath.Join(dstDir, nixoci.ImageMarker), false)
}

// TestPull_ManifestMissingFailsFast: a missing manifest is not retried,
// matching docker pull.
func TestPull_ManifestMissingFailsFast(t *testing.T) {
	setRetry(t, 5)
	buf := captureProgress(t)

	srv := newRegistry(t)
	defer srv.Close()
	imgRef := mustHost(t, srv.URL) + "/no-such:image"

	if err := nixoci.Pull(context.Background(), imgRef, t.TempDir(), 0); err == nil {
		t.Fatal("expected Pull to fail for missing manifest")
	}
	if strings.Contains(buf.String(), "attempt") {
		t.Errorf("missing manifest must not be retried; progress log:\n%s", buf.String())
	}
}

// Silence unused-import lints — these are kept for symmetry with future
// tests on push side.
var _ = crane.Pull
