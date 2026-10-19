# Copies the AWS-LC FIPS module into a profile's directory, beside `smeltr.exe`, and returns its path. AWS-LC builds
# its FIPS module statically only on Linux, so on Windows it is a DLL the binary loads, and Windows looks for it
# beside the executable. The newest build wins: a stale `aws-lc-fips-sys-*` directory can outlive an upgrade.
param([string]$ProfileDir)
$ErrorActionPreference = 'Stop'

$dll = Get-ChildItem (Join-Path $ProfileDir 'build') -Recurse -Filter 'aws_lc_fips_*_crypto.dll' |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $dll) { throw "no AWS-LC FIPS DLL under $ProfileDir/build" }
Copy-Item $dll.FullName $ProfileDir
Join-Path $ProfileDir $dll.Name
