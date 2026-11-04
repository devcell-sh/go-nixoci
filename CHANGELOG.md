# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-11-04

### Added

- feat(push): check available disk space before splitting and fail fast with a clear error if there isn't enough room — large pushes no longer die partway through with "no space left on device"
- docs(changelog): add CHANGELOG.md covering v0.1.0 through v0.4.0 — gives users and integrators a single place to see what changed per release without visiting GitHub

### Fixed

- fix(push): reset TotalSizeHint at the start of each Push call — repeated pushes in the same process no longer show progress percentages carried over from a previous push

### Changed

- perf(push): interleave chunk splitting with compression, deleting each uncompressed temp file immediately after compressing it — peak disk usage drops from ~2x the store size to one chunk plus the compressed total
- test(push): loosen repeat-push assertion to match the updated "new"/"exists" compression status wording

## [0.3.0] - 2026-11-04

### Added

- feat(push): pre-compress chunks to disk and HEAD-check blob digests before upload — re-pushing unchanged content skips blobs already in the registry instead of re-uploading the full store every time
- feat(push): expose ChunkBytes and UploadJobs on PushOpts — callers can tune chunk size and upload concurrency per push instead of only via package-level vars
- test(push): add repeat-push test asserting skipped blobs are reported in progress output

### Changed

- perf(push): raise default UploadJobs from 4 to 6 — uploads more blobs concurrently while staying under GHCR's ~2k req/min rate limit

## [0.2.0] - 2026-10-15

### Added

- feat(pull): commit each layer with a marker file after extraction and DiffID verification — a failed or interrupted pull resumes from the last committed layer instead of re-downloading everything
- feat(pull): verify each layer's uncompressed sha256 against the config DiffID — corrupted or truncated downloads are caught before the layer is committed
- feat(pull): retry failed layers up to 5 times with backoff — transient registry errors no longer abort the whole pull
- test(pull): add marker, resume, retry, DiffID mismatch, symlink overwrite, and volume marker tests

### Fixed

- fix(pull): remove existing symlinks and hardlinks before re-creating on retry — re-applying a partially extracted layer no longer fails on "file exists"

### Changed

- refactor(pull): extract fetchLayers/applyLayer/dockerRun helpers shared by Pull and PullToDockerVolume — no user-facing impact
- chore(gitignore): add .devcell.toml and .devcell/ — no user-facing impact

## [0.1.0] - 2026-08-27

### Added

- feat(push): push a nix store closure as chunked OCI image layers to any container registry
- feat(pull): pull and extract nix store layers from a registry into a local directory or Docker volume
- feat(push): configurable chunk size, upload concurrency, stall timeout, and retry with exponential backoff
- feat(pull): PullToDockerVolume streams layers directly into a named Docker volume via alpine container
- feat(resolve): resolve nix store paths from installable expressions via `nix path-info`

[Unreleased]: https://github.com/devcell-sh/go-nixoci/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/devcell-sh/go-nixoci/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/devcell-sh/go-nixoci/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/devcell-sh/go-nixoci/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/devcell-sh/go-nixoci/commits/v0.1.0
