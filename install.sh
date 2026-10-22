#!/bin/sh
# Installs the latest emberkv release.
#
#   curl -fsSL https://raw.githubusercontent.com/1001harshit/emberkv/main/install.sh | sh
#
# Environment variables:
#   EMBERKV_VERSION      release tag to install, e.g. v0.1.0 (default: latest)
#   EMBERKV_INSTALL_DIR  where to put the binaries (default: /usr/local/bin if
#                        writable, else ~/.local/bin)
set -eu

REPO="1001harshit/emberkv"
VERSION="${EMBERKV_VERSION:-latest}"
INSTALL_DIR="${EMBERKV_INSTALL_DIR:-}"

say() { printf 'emberkv: %s\n' "$*"; }
fail() { say "$*" >&2; exit 1; }

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  linux | darwin) ;;
  *) fail "unsupported OS '$os'. On Windows, download the zip from https://github.com/$REPO/releases" ;;
esac

arch=$(uname -m)
case "$arch" in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) fail "unsupported CPU architecture '$arch'" ;;
esac

if [ "$VERSION" = latest ]; then
  base="https://github.com/$REPO/releases/latest/download"
else
  base="https://github.com/$REPO/releases/download/$VERSION"
fi
# For testing against a local mirror of a release.
base="${EMBERKV_DOWNLOAD_URL:-$base}"

download() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  else
    fail "need curl or wget"
  fi
}

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

asset="emberkv_${os}_${arch}.tar.gz"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

say "downloading $asset ($VERSION)"
download "$base/$asset" "$tmp/$asset" || fail "download failed: $base/$asset"
download "$base/checksums.txt" "$tmp/checksums.txt" || fail "could not download checksums.txt"

expected=$(awk -v f="$asset" '$2 == f {print $1}' "$tmp/checksums.txt")
[ -n "$expected" ] || fail "$asset is not listed in checksums.txt"
[ "$expected" = "$(sha256 "$tmp/$asset")" ] || fail "checksum mismatch for $asset; aborting"

tar -xzf "$tmp/$asset" -C "$tmp"

if [ -z "$INSTALL_DIR" ]; then
  if [ -w /usr/local/bin ]; then
    INSTALL_DIR=/usr/local/bin
  else
    INSTALL_DIR="$HOME/.local/bin"
  fi
fi
mkdir -p "$INSTALL_DIR"
for bin in emberkv emberbench; do
  cp "$tmp/$bin" "$INSTALL_DIR/$bin"
  chmod 755 "$INSTALL_DIR/$bin"
done

say "installed $("$INSTALL_DIR/emberkv" -version) to $INSTALL_DIR"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) say "add $INSTALL_DIR to your PATH, e.g.: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac
say "start it with: emberkv -dir ~/.emberkv   (then connect any Redis client to 127.0.0.1:6380)"
