#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "${test_root}"' EXIT

version="2.0.0-rc.1"
tag="v${version}"
commit="0123456789abcdef0123456789abcdef01234567"
build_date="2026-08-17T12:34:56Z"
case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    aarch64 | arm64) arch="arm64" ;;
    armv7* | armhf) arch="arm" ;;
    *) echo "unsupported test architecture: $(uname -m)" >&2; exit 1 ;;
esac
binary_name="chronoqueue-${tag}-linux-${arch}"
release_dir="${test_root}/releases/download/${tag}"
staging_dir="${test_root}/staging"
install_dir="${test_root}/install"
mkdir -p "${release_dir}" "${staging_dir}"

(
    cd "${repository_root}"
    CGO_ENABLED=0 go build -trimpath \
        -ldflags="-X github.com/adrien19/chronoqueue/pkg/version.Version=${version} -X github.com/adrien19/chronoqueue/pkg/version.GitCommit=${commit} -X github.com/adrien19/chronoqueue/pkg/version.BuildDate=${build_date}" \
        -o "${staging_dir}/${binary_name}" .
)
printf 'version=%s\ncommit=%s\nbuild_date=%s\n' \
    "${tag}" "${commit}" "${build_date}" > "${staging_dir}/${binary_name}.release"
tar -C "${staging_dir}" -czf "${release_dir}/${binary_name}.tar.gz" \
    "${binary_name}" "${binary_name}.release"
(
    cd "${release_dir}"
    sha256sum "${binary_name}.tar.gz" > "${binary_name}.tar.gz.sha256"
)

CHRONOQUEUE_RELEASES_URL="file://${test_root}/releases" \
CHRONOQUEUE_INSTALL_DIR="${install_dir}" \
    bash "${repository_root}/install/install.sh" "${version}"

output="$("${install_dir}/chronoqueue" --version)"
grep -Fq "ChronoQueue v${version}" <<<"${output}"
grep -Fq "Git Commit: ${commit}" <<<"${output}"
grep -Fq "Built:      ${build_date}" <<<"${output}"

create_archive() {
    tar -C "${staging_dir}" -czf "${release_dir}/${binary_name}.tar.gz" \
        "${binary_name}" "${binary_name}.release"
    (
        cd "${release_dir}"
        sha256sum "${binary_name}.tar.gz" > "${binary_name}.tar.gz.sha256"
    )
}

printf 'version=v2.0.0\ncommit=%s\nbuild_date=%s\n' \
    "${commit}" "${build_date}" > "${staging_dir}/${binary_name}.release"
create_archive
if CHRONOQUEUE_RELEASES_URL="file://${test_root}/releases" \
    CHRONOQUEUE_INSTALL_DIR="${install_dir}" \
    bash "${repository_root}/install/install.sh" "${version}"; then
    echo "mismatched release tag was accepted" >&2
    exit 1
fi

printf 'version=%s\ncommit=abcdefabcdefabcdefabcdefabcdefabcdefabcd\nbuild_date=%s\n' \
    "${tag}" "${build_date}" > "${staging_dir}/${binary_name}.release"
create_archive
if CHRONOQUEUE_RELEASES_URL="file://${test_root}/releases" \
    CHRONOQUEUE_INSTALL_DIR="${install_dir}" \
    bash "${repository_root}/install/install.sh" "${version}"; then
    echo "mismatched release commit was accepted" >&2
    exit 1
fi

if CHRONOQUEUE_RELEASES_URL="file://${test_root}/releases" \
    CHRONOQUEUE_INSTALL_DIR="${install_dir}" \
    bash "${repository_root}/install/install.sh" "not-semver"; then
    echo "invalid version was accepted" >&2
    exit 1
fi

if CHRONOQUEUE_RELEASES_URL="file://${test_root}/releases" \
    CHRONOQUEUE_INSTALL_DIR="${install_dir}" \
    bash "${repository_root}/install/install.sh" "2.0.0-01"; then
    echo "invalid semantic version was accepted" >&2
    exit 1
fi
