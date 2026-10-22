# Releasing

Releases are built by [GoReleaser](https://goreleaser.com) in
[`.github/workflows/release.yml`](.github/workflows/release.yml) whenever a
`v*` tag is pushed:

```bash
git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```

That one push:

1. runs the tests,
2. builds `emberkv` and `emberbench` for Linux, macOS and Windows (amd64 and arm64),
3. publishes a GitHub release with the archives and `checksums.txt`, which is what
   `install.sh` downloads,
4. pushes a multi-arch image to `ghcr.io/1001harshit/emberkv` (`:<version>` and `:latest`),
5. updates the Homebrew formula in
   [1001harshit/homebrew-tap](https://github.com/1001harshit/homebrew-tap), **if** the
   repository secret `HOMEBREW_TAP_GITHUB_TOKEN` is set.

## One-time setup for Homebrew updates

The workflow's built-in token can't push to another repository. To automate
step 5, create a fine-grained personal access token with **Contents: Read and
write** on `1001harshit/homebrew-tap` only, then add it to this repository as
the secret `HOMEBREW_TAP_GITHUB_TOKEN` (Settings → Secrets and variables →
Actions). Until then, everything else is still released.

## Trying a release locally

```bash
go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean --skip=docker
```

The output goes to `dist/`, which is not committed.
