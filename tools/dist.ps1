# `just dist` on Windows: the binary, the AWS-LC FIPS DLL it loads, the license and the notices in
# `smeltr-windows.zip`, and SHA256SUMS over it. From any branch with nothing uncommitted, like the Linux recipe,
# whose comment says why the name carries no version.
param([string]$Version)
$ErrorActionPreference = 'Stop'

$changes = git status --porcelain
if ($LASTEXITCODE -ne 0) { exit 1 }
if ($changes) { throw 'dist builds a commit: commit or stash what is uncommitted' }
cargo auditable build --locked --profile dist --bin smeltr
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Windows PowerShell 5.1, which `just dist` runs, has no `??`.
$target = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { 'target' }
$exe = Join-Path $target 'dist/smeltr.exe'
$v = (Select-String -Path $exe -Pattern $Version -Encoding ascii | Select-Object -First 1).Matches[0].Value
if (-not $v) { throw "no version in $exe, so build.rs read no repository" }
$dll = & (Join-Path $PSScriptRoot 'fips-dll.ps1') (Join-Path $target 'dist')
$zip = 'smeltr-windows.zip'
Compress-Archive -Force -Path $exe, $dll, LICENSE, THIRD-PARTY-NOTICES.md -DestinationPath $zip
# Not `Get-FileHash`: under the pwsh 7 that starts it on a runner, Windows PowerShell inherits pwsh's module path and
# cannot load the module that defines it.
$sha = [Security.Cryptography.SHA256]::Create().ComputeHash([IO.File]::ReadAllBytes((Resolve-Path $zip)))
"$(-join ($sha | ForEach-Object { $_.ToString('x2') }))  $zip" | Set-Content SHA256SUMS
