# Puts the x64 MSVC environment a Developer PowerShell sets (PATH with nmake and cl, INCLUDE, LIB) into the GitHub
# job's environment, for the steps after this one.
$ErrorActionPreference = 'Stop'
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
$vs = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
Import-Module (Join-Path $vs 'Common7\Tools\Microsoft.VisualStudio.DevShell.dll')
Enter-VsDevShell -VsInstallPath $vs -SkipAutomaticLocation -DevCmdArguments '-arch=x64 -host_arch=x64'
foreach ($name in 'PATH', 'INCLUDE', 'LIB', 'LIBPATH') {
    "$name=$([Environment]::GetEnvironmentVariable($name))" | Out-File -Append -Encoding utf8 $env:GITHUB_ENV
}
