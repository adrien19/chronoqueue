
# ChronoQueue install script for Windows (PowerShell)
#
# Usage:
#   # Install latest version to $Env:SystemDrive\chronoqueue and add to User PATH
#   powershell -Command "iwr -useb https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.ps1 | iex"
#
#   # Install a specific version
#   $s=iwr -useb https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.ps1; `
#   $b=[ScriptBlock]::Create($s); invoke-command -ScriptBlock $b -ArgumentList '0.1.0'
#
#   # Install to a custom directory (no admin required)
#   $Env:CHRONOQUEUE_INSTALL_DIR="C:\tools\chronoqueue"
#   powershell -Command "iwr -useb https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.ps1 | iex"
#
#   # Install a specific version to a custom directory
#   $s=iwr -useb https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.ps1; `
#   $b=[ScriptBlock]::Create($s); invoke-command -ScriptBlock $b -ArgumentList '0.1.0','C:\tools\chronoqueue'

param (
    # Version to install (e.g. "0.1.0" or "v0.1.0"). Defaults to latest release.
    [string]$Version  = "",
    # Directory to install chronoqueue into. Defaults to $Env:CHRONOQUEUE_INSTALL_DIR
    # or $Env:SystemDrive\chronoqueue if not set.
    [string]$InstallDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Constants ─────────────────────────────────────────────────────────────────

$GithubOrg   = "adrien19"
$GithubRepo  = "chronoqueue"
$BinaryName  = "chronoqueue.exe"
$ReleasesUrl = if ($Env:CHRONOQUEUE_RELEASES_URL) { $Env:CHRONOQUEUE_RELEASES_URL } else { "https://github.com/$GithubOrg/$GithubRepo/releases" }
$ApiUrl      = if ($Env:CHRONOQUEUE_API_URL) { $Env:CHRONOQUEUE_API_URL } else { "https://api.github.com/repos/$GithubOrg/$GithubRepo/releases/latest" }

# ── Helpers ───────────────────────────────────────────────────────────────────

function Write-Step([string]$Message) {
    Write-Host "==> $Message" -ForegroundColor Green
}

function Write-Warn([string]$Message) {
    Write-Host "WARN $Message" -ForegroundColor Yellow
}

function Exit-Error([string]$Message) {
    Write-Host "ERROR $Message" -ForegroundColor Red
    exit 1
}

function Assert-SemanticVersion([string]$Value) {
    if ($Value -notmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(\+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$') {
        Exit-Error "Invalid version '$Value'. Expected a semantic version such as 2.0.0 or 2.0.0-rc.1."
    }
}

function Get-MetadataValue([string]$MetadataFile, [string]$Field) {
    $prefix = "${Field}="
    $line = Get-Content -Path $MetadataFile | Where-Object { $_.StartsWith($prefix) } | Select-Object -First 1
    if ($null -eq $line) {
        return ""
    }
    return $line.Substring($prefix.Length)
}

function Test-ReleaseMetadata([string]$BinaryPath, [string]$MetadataFile, [string]$RequestedVersion) {
    $releaseVersion = Get-MetadataValue -MetadataFile $MetadataFile -Field "version"
    $releaseCommit = Get-MetadataValue -MetadataFile $MetadataFile -Field "commit"
    $releaseBuildDate = Get-MetadataValue -MetadataFile $MetadataFile -Field "build_date"

    if ($releaseVersion -ne "v$RequestedVersion") {
        Exit-Error "Release metadata version '$releaseVersion' does not match requested version 'v$RequestedVersion'."
    }
    if ($releaseCommit -notmatch '^[0-9a-fA-F]{40}$') {
        Exit-Error "Release metadata contains an invalid commit '$releaseCommit'."
    }
    if ([string]::IsNullOrWhiteSpace($releaseBuildDate)) {
        Exit-Error "Release metadata does not contain a build date."
    }

    $output = (& $BinaryPath --version | Out-String)
    if ($LASTEXITCODE -ne 0) {
        Exit-Error "Installed binary failed its version check."
    }
    if (-not $output.Contains("ChronoQueue v$RequestedVersion")) {
        Exit-Error "Installed binary does not report version 'v$RequestedVersion'."
    }
    if (-not $output.Contains("Git Commit: $releaseCommit")) {
        Exit-Error "Installed binary commit does not match the release metadata."
    }
    if (-not $output.Contains("Built:      $releaseBuildDate")) {
        Exit-Error "Installed binary build date does not match the release metadata."
    }

    Write-Step "Release metadata verified (commit $($releaseCommit.Substring(0, 12)))."
}

# ── Resolve latest version from GitHub API ────────────────────────────────────

function Get-LatestVersion {
    try {
        $response = Invoke-RestMethod -Uri $ApiUrl `
            -Headers @{ Accept = "application/vnd.github+json" } `
            -UseBasicParsing
        $tag = $response.tag_name -replace '^v', ''
        if ([string]::IsNullOrWhiteSpace($tag)) {
            Exit-Error "Could not parse version from GitHub API response."
        }
        return $tag
    } catch {
        Exit-Error "Failed to contact GitHub API: $_"
    }
}

# ── Verify SHA256 checksum ────────────────────────────────────────────────────

function Test-Checksum([string]$FilePath, [string]$ChecksumFile) {
    # Read expected checksum from .sha256 file (first field)
    $expected = (Get-Content -Path $ChecksumFile -First 1).Split()[0].Trim().ToLower()

    if ([string]::IsNullOrWhiteSpace($expected)) {
        Exit-Error "Failed to read checksum from checksum file."
    }

    $actual = (Get-FileHash -Path $FilePath -Algorithm SHA256).Hash.ToLower()

    if ($expected -ne $actual) {
        Exit-Error "Checksum mismatch.`n  expected: $expected`n  actual:   $actual`nAborting installation."
    }
    Write-Step "Checksum verified."
}

# ── Main ──────────────────────────────────────────────────────────────────────

function Install-ChronoQueue {
    # ── Version ──────────────────────────────────────────────────────────────
    $ver = $Version.TrimStart('v')
    if ([string]::IsNullOrWhiteSpace($ver)) {
        Write-Step "Determining latest chronoqueue version..."
        $ver = Get-LatestVersion
    }
    Assert-SemanticVersion -Value $ver
    Write-Step "Installing chronoqueue v$ver"

    # ── Architecture (Windows releases only ship amd64) ───────────────────────
    $arch = "amd64"
    $archiveName = "chronoqueue-v${ver}-windows-${arch}.zip"
    $binaryInArchive = "chronoqueue-v${ver}-windows-${arch}.exe"

    # ── Install dir ──────────────────────────────────────────────────────────
    $userSetDir = $false
    if (-not [string]::IsNullOrWhiteSpace($InstallDir)) {
        $userSetDir = $true
    } elseif (-not [string]::IsNullOrWhiteSpace($Env:CHRONOQUEUE_INSTALL_DIR)) {
        $InstallDir = $Env:CHRONOQUEUE_INSTALL_DIR
        $userSetDir = $true
    } else {
        $InstallDir = Join-Path $Env:LOCALAPPDATA "Programs\chronoqueue"
    }

    # ── URLs ──────────────────────────────────────────────────────────────────
    $baseUrl     = "$ReleasesUrl/download/v$ver"
    $archiveUrl  = "$baseUrl/$archiveName"
    $checksumUrl = "$baseUrl/${archiveName}.sha256"

    # ── Temporary work dir ────────────────────────────────────────────────────
    $tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())
    New-Item -ItemType Directory -Path $tmpDir | Out-Null

    try {
        $archivePath   = Join-Path $tmpDir $archiveName
        $checksumPath  = Join-Path $tmpDir "${archiveName}.sha256"

        # ── Download ──────────────────────────────────────────────────────────
        Write-Step "Downloading $archiveName..."
        try {
            Invoke-WebRequest -Uri $archiveUrl -OutFile $archivePath -UseBasicParsing
        } catch {
            Exit-Error "Failed to download archive from: $archiveUrl`n$_"
        }

        Write-Step "Downloading checksum file..."
        try {
            Invoke-WebRequest -Uri $checksumUrl -OutFile $checksumPath -UseBasicParsing
        } catch {
            Exit-Error "Failed to download checksum from: $checksumUrl`n$_"
        }

        # ── Verify ────────────────────────────────────────────────────────────
        Test-Checksum -FilePath $archivePath -ChecksumFile $checksumPath

        # ── Extract ───────────────────────────────────────────────────────────
        Write-Step "Extracting..."
        $extractDir = Join-Path $tmpDir "extract"
        Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force

        $binaryPath = Join-Path $extractDir $binaryInArchive
        if (-not (Test-Path $binaryPath)) {
            Exit-Error "Binary '$binaryInArchive' not found in archive."
        }

        $metadataInArchive = "${binaryInArchive}.release"
        $metadataPath = Join-Path $extractDir $metadataInArchive
        if (-not (Test-Path $metadataPath)) {
            Exit-Error "Release metadata '$metadataInArchive' not found in archive."
        }
        Test-ReleaseMetadata -BinaryPath $binaryPath -MetadataFile $metadataPath -RequestedVersion $ver

        # ── Install ───────────────────────────────────────────────────────────
        if (-not (Test-Path $InstallDir)) {
            New-Item -ItemType Directory -Path $InstallDir | Out-Null
        }
        Write-Step "Installing chronoqueue to $InstallDir..."
        Copy-Item -Path $binaryPath -Destination (Join-Path $InstallDir $BinaryName) -Force

        # ── Add to User PATH ──────────────────────────────────────────────────
        $currentPath = [System.Environment]::GetEnvironmentVariable("PATH", "User")
        $pathEntries = $currentPath -split ';' | Where-Object { $_ -ne '' }

        if ($pathEntries -notcontains $InstallDir) {
            $newPath = ($pathEntries + $InstallDir) -join ';'
            [System.Environment]::SetEnvironmentVariable("PATH", $newPath, "User")
            Write-Step "Added '$InstallDir' to your User PATH."
            Write-Warn "Restart your terminal for the PATH change to take effect."
        } else {
            Write-Step "'$InstallDir' is already on your User PATH."
        }

        # ── Done ──────────────────────────────────────────────────────────────
        Write-Step "chronoqueue v$ver installed successfully."
        Write-Host ""
        Write-Host "Run 'chronoqueue --help' to get started." -ForegroundColor Cyan

    } finally {
        Remove-Item -Recurse -Force -Path $tmpDir -ErrorAction SilentlyContinue
    }
}

Install-ChronoQueue
