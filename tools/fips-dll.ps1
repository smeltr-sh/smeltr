# Copies the AWS-LC FIPS module into a profile's directory, beside `smeltr.exe`, and beside each test binary Cargo
# has built (in `deps`, or each `build/<crate>/<hash>/out` in the layout Cargo 1.100 builds), and returns its path in
# the profile's directory. A test binary built after this runs has none until it runs again. AWS-LC builds its FIPS
# module statically only on Linux, so on Windows it is a DLL the binary loads, and Windows looks for it beside the
# executable. The newest build wins: a stale `aws-lc-fips-sys-*` directory can outlive an upgrade.
param([string]$ProfileDir)
$ErrorActionPreference = 'Stop'

$dll = Get-ChildItem (Join-Path $ProfileDir 'build') -Recurse -Filter 'aws_lc_fips_*_crypto.dll' |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $dll) { throw "no AWS-LC FIPS DLL under $ProfileDir/build" }
$tests = Get-ChildItem (Join-Path $ProfileDir 'build') -Directory -Filter 'out' -Recurse -Depth 2 |
  Where-Object { Get-ChildItem $_.FullName -Filter '*.exe' | Where-Object Name -NotLike 'build_script_*' } |
  ForEach-Object FullName
foreach ($dir in @($ProfileDir, (Join-Path $ProfileDir 'deps')) + @($tests)) {
  if (-not (Test-Path $dir)) { continue }
  $to = Join-Path $dir $dll.Name
  # Checks run side by side, each copying first: one already there and as new is the same file, and may be loaded.
  if ((Test-Path $to) -and (Get-Item $to).LastWriteTime -ge $dll.LastWriteTime) { continue }
  Copy-Item $dll.FullName $dir
}
Join-Path $ProfileDir $dll.Name
