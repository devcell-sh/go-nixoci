# nixoci

Push and pull a Nix store as an OCI image layer. nixoci serializes a populated `/nix` into a single layer, ships it to a registry like GHCR, and restores it on the other side. The point is caching: rebuilding a Nix environment from scratch takes minutes to hours, pulling a layer takes seconds.

It talks to registries through `go-containerregistry` directly rather than shelling out to `crane`. That started as a fix (crane's stdin handling re-gzipped pre-gzipped input and buffered to disk, which broke CI in creative ways) and ended up as the better design: deterministic encoding, true streaming, and one code path shared by tests and CI.

nixoci is used by [devcell](https://github.com/DimmKirr/devcell) to cache container Nix volumes between CI runs. The API is `Push`, `Pull`, and `ResolveImage`; pin a version if you build on it.

```sh
go get github.com/dimmkirr/nixoci
```
