#!/usr/bin/env bash
# ChronoQueue install script
#
# Usage:
#   # Install latest version to /usr/local/bin (uses sudo if required)
#   curl -fsSL https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.sh | bash
#
#   # Install a specific version
#   curl -fsSL https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.sh | bash -s 0.1.0
#
#   # Install to a custom directory (no sudo)
#   curl -fsSL https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.sh | CHRONOQUEUE_INSTALL_DIR="$HOME/.chronoqueue" bash
#
#   # Install a specific version to a custom directory (no sudo)
#   curl -fsSL https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.sh | CHRONOQUEUE_INSTALL_DIR="$HOME/.chronoqueue" bash -s 0.1.0

set -euo pipefail

# ── Constants ────────────────────────────────────────────────────────────────

GITHUB_ORG="adrien19"
GITHUB_REPO="chronoqueue"
BINARY_NAME="chronoqueue"
RELEASES_URL="https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases"
API_URL="https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest"

# ── Helpers ──────────────────────────────────────────────────────────────────

say() { printf "\033[1;32m==>\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33mWARN\033[0m %s\n" "$*" >&2; }
err() { printf "\033[1;31mERROR\033[0m %s\n" "$*" >&2; exit 1; }

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        err "Required command not found: '$1'. Please install it and retry."
    fi
}

# ── Detect OS ─────────────────────────────────────────────────────────────────

detect_os() {
    local os
    os="$(uname -s)"
    case "${os}" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       err "Unsupported OS: ${os}. Only Linux and macOS are supported." ;;
    esac
}

# ── Detect architecture ───────────────────────────────────────────────────────

detect_arch() {
    local arch
    arch="$(uname -m)"
    case "${arch}" in
        x86_64)           echo "amd64" ;;
        aarch64 | arm64)  echo "arm64" ;;
        armv7* | armhf)   echo "arm" ;;
        *)                err "Unsupported architecture: ${arch}." ;;
    esac
}

# ── Resolve latest version from GitHub API ───────────────────────────────────

latest_version() {
    need_cmd curl
    local version
    version="$(curl -fsSL "${API_URL}" \
        -H "Accept: application/vnd.github+json" \
        | grep '"tag_name"' \
        | sed -E 's/.*"tag_name": *"v?([^"]+)".*/\1/' \
        | head -1)"

    if [[ -z "${version}" ]]; then
        err "Failed to determine the latest ${BINARY_NAME} version from GitHub. Check your network connection."
    fi
    echo "${version}"
}

# ── Verify SHA256 checksum ────────────────────────────────────────────────────

verify_checksum() {
    local archive="$1"
    local checksum_file="$2"

    # Read expected checksum from .sha256 file
    local expected
    expected="$(awk '{print $1}' "${checksum_file}")"

    if [[ -z "${expected}" ]]; then
        err "Failed to read checksum from checksum file."
    fi

    # Platform-specific sha256 command
    local actual
    if command -v sha256sum >/dev/null 2>&1; then
        actual="$(sha256sum "${archive}" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
        actual="$(shasum -a 256 "${archive}" | awk '{print $1}')"
    else
        err "Neither 'sha256sum' nor 'shasum' found. Cannot verify checksum."
    fi

    if [[ "${expected}" != "${actual}" ]]; then
        err "Checksum mismatch.
  expected: ${expected}
  actual:   ${actual}
Aborting installation."
    fi

    say "Checksum verified."
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    need_cmd curl
    need_cmd tar

    # ── Version ──────────────────────────────────────────────────────────────
    local version="${1:-}"
    # Strip leading 'v' if present
    version="${version#v}"
    if [[ -z "${version}" ]]; then
        say "Determining latest ${BINARY_NAME} version..."
        version="$(latest_version)"
    fi
    say "Installing ${BINARY_NAME} v${version}"

    # ── Target platform ──────────────────────────────────────────────────────
    local os arch
    os="$(detect_os)"
    arch="$(detect_arch)"

    local archive_name="${BINARY_NAME}-v${version}-${os}-${arch}.tar.gz"
    local checksum_url="${RELEASES_URL}/download/v${version}/${archive_name}.sha256"
    local archive_url="${RELEASES_URL}/download/v${version}/${archive_name}"

    # ── Install dir ──────────────────────────────────────────────────────────
    local install_dir="${CHRONOQUEUE_INSTALL_DIR:-/usr/local/bin}"
    local user_set_dir="${CHRONOQUEUE_INSTALL_DIR:+true}"

    # ── Temporary work dir (cleaned up on exit) ───────────────────────────────
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "${tmp_dir}"' EXIT

    # ── Download ──────────────────────────────────────────────────────────────
    say "Downloading ${archive_name}..."
    curl -fsSL --progress-bar -o "${tmp_dir}/${archive_name}" "${archive_url}" \
        || err "Failed to download archive from: ${archive_url}"

    say "Downloading checksum file..."
    curl -fsSL -o "${tmp_dir}/${archive_name}.sha256" "${checksum_url}" \
        || err "Failed to download checksum from: ${checksum_url}"

    # ── Verify ────────────────────────────────────────────────────────────────
    verify_checksum "${tmp_dir}/${archive_name}" "${tmp_dir}/${archive_name}.sha256"

    # ── Extract ───────────────────────────────────────────────────────────────
    say "Extracting..."
    tar -xzf "${tmp_dir}/${archive_name}" -C "${tmp_dir}"

    local binary_in_archive="${BINARY_NAME}-v${version}-${os}-${arch}"
    if [[ ! -f "${tmp_dir}/${binary_in_archive}" ]]; then
        err "Binary '${binary_in_archive}' not found in archive."
    fi
    chmod +x "${tmp_dir}/${binary_in_archive}"

    # ── Install ───────────────────────────────────────────────────────────────
    # Determine whether sudo is needed:
    # - Never use sudo if the user explicitly set CHRONOQUEUE_INSTALL_DIR
    # - Use sudo only if the default /usr/local/bin is not writable
    local use_sudo="false"
    if [[ "${user_set_dir}" != "true" ]] && [[ ! -w "${install_dir}" ]]; then
        use_sudo="true"
    fi

    if [[ "${use_sudo}" == "true" ]]; then
        sudo mkdir -p "${install_dir}"
        say "Installing ${BINARY_NAME} to ${install_dir} (sudo required)..."
        sudo install -m 0755 "${tmp_dir}/${binary_in_archive}" "${install_dir}/${BINARY_NAME}"
    else
        mkdir -p "${install_dir}"
        say "Installing ${BINARY_NAME} to ${install_dir}..."
        install -m 0755 "${tmp_dir}/${binary_in_archive}" "${install_dir}/${BINARY_NAME}"
    fi

    # ── Done ──────────────────────────────────────────────────────────────────
    say "${BINARY_NAME} v${version} installed successfully."

    # Warn if install dir is not on PATH
    if ! echo ":${PATH}:" | grep -q ":${install_dir}:"; then
        warn "'${install_dir}' is not on your PATH."
        warn "Add the following to your shell profile:"
        warn "  export PATH=\"\$PATH:${install_dir}\""
    fi

    say "Run '${BINARY_NAME} --help' to get started."
}

main "$@"
