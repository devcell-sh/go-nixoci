// Package nixoci implements push/pull of a nix-store tarball as an
// OCI image layer, against a remote registry like GHCR.
//
// Background: the CI workflow caches the populated `/nix` Docker volume
// between runs by serializing it as a single OCI layer. CELL-292 made
// this work via the `crane` CLI, but `crane append`'s stdin handler had
// behavior that caused six iterations of CI breakage (re-gzipping
// pre-gzipped input, buffering uncompressed stdin to disk, …). This
// package replaces those CLI calls with direct uses of
// `github.com/google/go-containerregistry/pkg/v1/{stream,tarball}`,
// giving deterministic encoding + true streaming + a single Go code
// path shared between local tests and CI.
package nixoci

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

const (
	alpineImage = "public.ecr.aws/docker/library/alpine:latest"

	// MarkerDir, relative to the destination root, holds one empty file
	// per applied layer, named by the layer's compressed sha256 hex.
	MarkerDir = ".nixoci/layers"
	// ImageMarker, relative to the destination root, holds the manifest
	// digest of the last fully applied image.
	ImageMarker = ".nixoci/image"
)

// MaxDownloadAttempts is how many times a single layer is streamed and
// applied before the pull fails, mirroring dockerd's
// --max-download-attempts. Retry is per layer: a failed layer is
// re-streamed from byte 0; committed layers are never re-downloaded.
var MaxDownloadAttempts = 5

// Pull resolves srcRef, downloads all non-base layers of the image,
// and extracts each gzipped tarball into dstDir. For multi-layer
// (chunked) images, layers[1:] are extracted in order; for single-layer
// images, layers[0] is extracted (backward compat with legacy cache
// images). Archive entries are written relative to dstDir, with
// stripComponents leading path elements stripped per `tar
// --strip-components=N` semantics. Symlinks and file modes are preserved.
//
// Commit/resume contract: each layer is applied, its uncompressed
// stream verified against the config DiffID, then recorded as
// MarkerDir/<digest-hex>. Layers with an existing marker are skipped,
// so a failed pull is resumed by calling Pull again. ImageMarker is
// written last and only once every layer is committed.
func Pull(ctx context.Context, srcRef, dstDir string, stripComponents int) error {
	img, layers, err := fetchLayers(ctx, srcRef)
	if err != nil {
		return err
	}

	applied, err := fsMarkers(dstDir)
	if err != nil {
		return err
	}
	for _, l := range layers {
		if applied[l.hex] {
			progressLog("[nix-store pull] layer %d already applied, skipping\n", l.index)
			continue
		}
		err := retryLayer(ctx, l, func() error {
			err := applyLayer(l, func(r io.Reader) error {
				return extractTar(r, dstDir, stripComponents)
			})
			if err != nil {
				return fmt.Errorf("extract layer %d into %q: %w", l.index, dstDir, err)
			}
			if err := syncWriteFile(filepath.Join(dstDir, MarkerDir, l.hex), nil); err != nil {
				return fmt.Errorf("commit layer %d: %w", l.index, err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	digest, err := img.Digest()
	if err != nil {
		return fmt.Errorf("image digest: %w", err)
	}
	return syncWriteFile(filepath.Join(dstDir, ImageMarker), []byte(digest.String()))
}

// PullToDockerVolume streams all non-base layers of srcRef into the
// named Docker volume by spawning `docker run -i alpine sh -c 'cd
// /dest && tar -x --strip-components=N'` for each layer and feeding
// the (gunzipped) tar stream over stdin. For multi-layer (chunked)
// images, layers[1:] are extracted in order; for single-layer images,
// layers[0] is extracted (backward compat).
//
// stripComponents has the same meaning as Pull: leading path elements
// to strip from each archive entry.
//
// Commit/resume contract matches Pull: MarkerDir/<digest-hex> is
// touched on the volume after a layer extracts and its DiffID verifies;
// marked layers are skipped on the next call; ImageMarker is written
// last. Marker writes are separate `docker run` invocations so a layer
// is never committed before Go has verified the stream it fed tar.
func PullToDockerVolume(ctx context.Context, srcRef, volName string, stripComponents int) error {
	img, layers, err := fetchLayers(ctx, srcRef)
	if err != nil {
		return err
	}

	tarFlag := ""
	if stripComponents > 0 {
		tarFlag = fmt.Sprintf(" --strip-components=%d", stripComponents)
	}

	applied, err := volumeMarkers(ctx, volName)
	if err != nil {
		return err
	}
	for _, l := range layers {
		if applied[l.hex] {
			progressLog("[nix-store pull] layer %d already applied, skipping\n", l.index)
			continue
		}
		err := retryLayer(ctx, l, func() error {
			err := applyLayer(l, func(r io.Reader) error {
				cmd := dockerRun(ctx, volName, "-i", "sh", "-c", "cd /dest && tar -x"+tarFlag)
				cmd.Stdin = r
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				return cmd.Run()
			})
			if err != nil {
				return fmt.Errorf("docker tar -x layer %d into %q: %w", l.index, volName, err)
			}
			if err := dockerRun(ctx, volName, "", "touch", "/dest/"+MarkerDir+"/"+l.hex).Run(); err != nil {
				return fmt.Errorf("commit layer %d: %w", l.index, err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	digest, err := img.Digest()
	if err != nil {
		return fmt.Errorf("image digest: %w", err)
	}
	cmd := dockerRun(ctx, volName, "", "sh", "-c", `printf %s "$1" > /dest/`+ImageMarker, "sh", digest.String())
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("write image marker: %w", err)
	}
	return nil
}

// dataLayer is a non-base layer of a cache image plus its position in
// the manifest and the validated hex of its compressed digest.
type dataLayer struct {
	v1.Layer
	index int
	hex   string
}

func fetchLayers(ctx context.Context, srcRef string) (v1.Image, []dataLayer, error) {
	ref, err := name.ParseReference(srcRef)
	if err != nil {
		return nil, nil, fmt.Errorf("parse %q: %w", srcRef, err)
	}
	img, err := remote.Image(ref,
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(authn.DefaultKeychain),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch manifest %q: %w", srcRef, err)
	}
	layers, err := img.Layers()
	if err != nil {
		return nil, nil, fmt.Errorf("read layers: %w", err)
	}
	if len(layers) == 0 {
		return nil, nil, fmt.Errorf("image %q has no layers", srcRef)
	}

	start := 0
	if len(layers) > 1 {
		start = 1
	}
	out := make([]dataLayer, 0, len(layers)-start)
	for i, l := range layers[start:] {
		d, err := l.Digest()
		if err != nil {
			return nil, nil, fmt.Errorf("layer %d digest: %w", start+i, err)
		}
		if d.Algorithm != "sha256" || len(d.Hex) != 64 || !isHex(d.Hex) {
			return nil, nil, fmt.Errorf("layer %d: unsupported digest %q", start+i, d)
		}
		out = append(out, dataLayer{Layer: l, index: start + i, hex: d.Hex})
	}
	return img, out, nil
}

func retryLayer(ctx context.Context, l dataLayer, fn func() error) error {
	label := fmt.Sprintf("[nix-store pull] layer %d", l.index)
	return withRetry(ctx, label, MaxDownloadAttempts, fn)
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

// applyLayer feeds the layer's uncompressed stream to extract and then
// checks the bytes extract consumed against the layer's DiffID. The
// stream is drained after extract returns so the hash covers the whole
// layer even if extract stopped at the tar end-of-archive marker.
func applyLayer(l dataLayer, extract func(io.Reader) error) error {
	want, err := l.DiffID()
	if err != nil {
		return fmt.Errorf("diff id: %w", err)
	}
	rc, err := l.Uncompressed()
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	defer rc.Close()

	h := sha256.New()
	tee := io.TeeReader(rc, h)
	if err := extract(tee); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return fmt.Errorf("drain stream: %w", err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if want.Algorithm != "sha256" || got != want.Hex {
		return fmt.Errorf("diff id mismatch: config %s, stream sha256:%s", want, got)
	}
	return nil
}

func fsMarkers(dstDir string) (map[string]bool, error) {
	entries, err := os.ReadDir(filepath.Join(dstDir, MarkerDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read markers: %w", err)
	}
	set := make(map[string]bool, len(entries))
	for _, e := range entries {
		set[e.Name()] = true
	}
	return set, nil
}

func volumeMarkers(ctx context.Context, volName string) (map[string]bool, error) {
	cmd := dockerRun(ctx, volName, "", "sh", "-c", "mkdir -p /dest/"+MarkerDir+" && ls -1 /dest/"+MarkerDir)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("read markers on %q: %w", volName, err)
	}
	set := map[string]bool{}
	for _, line := range bytes.Split(out, []byte("\n")) {
		if name := strings.TrimSpace(string(line)); name != "" {
			set[name] = true
		}
	}
	return set, nil
}

// dockerRun builds `docker run --rm [flag] -v vol:/dest alpine args...`.
// flag is an optional extra docker flag such as "-i".
func dockerRun(ctx context.Context, volName, flag string, args ...string) *osexec.Cmd {
	argv := []string{"run", "--rm"}
	if flag != "" {
		argv = append(argv, flag)
	}
	argv = append(argv, "-v", volName+":/dest", alpineImage)
	argv = append(argv, args...)
	return osexec.CommandContext(ctx, "docker", argv...)
}

func syncWriteFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// extractTar reads a tar stream from r and writes each entry under
// dstDir, with stripComponents leading path elements stripped per
// `tar --strip-components=N` semantics. dstDir must exist. Supports
// regular files, directories, symlinks, and hardlinks (the file types
// Nix actually uses in /nix/store).
func extractTar(r io.Reader, dstDir string, stripComponents int) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("tar next: %w", err)
		}
		stripped, skip := stripPath(hdr.Name, stripComponents)
		if skip {
			continue
		}
		target, err := safeJoin(dstDir, stripped)
		if err != nil {
			return fmt.Errorf("unsafe entry %q: %w", hdr.Name, err)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)&0o7777); err != nil {
				return fmt.Errorf("mkdir %q: %w", target, err)
			}
		case tar.TypeReg, tar.TypeRegA: //nolint:staticcheck // RegA still appears in older archives
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("mkdir parent of %q: %w", target, err)
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o7777)
			if err != nil {
				return fmt.Errorf("create %q: %w", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("write %q: %w", target, err)
			}
			if err := f.Close(); err != nil {
				return fmt.Errorf("close %q: %w", target, err)
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("mkdir parent of %q: %w", target, err)
			}
			if err := removeIfExists(target); err != nil {
				return err
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return fmt.Errorf("symlink %q -> %q: %w", target, hdr.Linkname, err)
			}
		case tar.TypeLink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("mkdir parent of %q: %w", target, err)
			}
			linkTarget, err := safeJoin(dstDir, hdr.Linkname)
			if err != nil {
				return fmt.Errorf("unsafe hardlink target %q: %w", hdr.Linkname, err)
			}
			if err := removeIfExists(target); err != nil {
				return err
			}
			if err := os.Link(linkTarget, target); err != nil {
				return fmt.Errorf("hardlink %q -> %q: %w", target, linkTarget, err)
			}
		default:
			// Skip char/block devices, FIFOs, etc. — nix-store doesn't
			// use them; the workflow's tar already excludes
			// nix/var/nix/daemon-socket.
			continue
		}
	}
}

// removeIfExists unlinks target so re-applying a partially extracted
// layer can recreate links that already landed on the first attempt.
func removeIfExists(target string) error {
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove existing %q: %w", target, err)
	}
	return nil
}

// stripPath drops the first n path elements from p (per `tar
// --strip-components=N` semantics). Returns (stripped, skip) where
// skip is true when the entry should be discarded because it has
// fewer than n components — matches GNU tar's behavior.
func stripPath(p string, n int) (string, bool) {
	if n <= 0 {
		return p, false
	}
	parts := strings.Split(filepath.ToSlash(p), "/")
	if len(parts) <= n {
		return "", true
	}
	return strings.Join(parts[n:], "/"), false
}

// safeJoin prevents zip-slip: cleans the relative path and verifies the
// result stays inside dstDir.
func safeJoin(dstDir, rel string) (string, error) {
	cleaned := filepath.Clean(rel)
	if strings.HasPrefix(cleaned, "..") || filepath.IsAbs(cleaned) {
		return "", errors.New("path escapes destination")
	}
	abs := filepath.Join(dstDir, cleaned)
	absRoot, err := filepath.Abs(dstDir)
	if err != nil {
		return "", err
	}
	absTarget, err := filepath.Abs(abs)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absTarget, absRoot+string(filepath.Separator)) && absTarget != absRoot {
		return "", errors.New("path escapes destination")
	}
	return abs, nil
}
