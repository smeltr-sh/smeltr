#!/bin/sh
# Installs a root filesystem from Wolfi's packages, for `Dockerfile` and `Dockerfile.handoff`:
#
#     wolfi-root.sh builder <root>   what compiles the binary
#     wolfi-root.sh image <root>     what the binary runs on
#
# It runs under Alpine's apk, which reads Wolfi's repository. The key it trusts is the one the caller put in
# /etc/apk/keys. Package scripts do not run: they would run Alpine's tools against Wolfi's files.
set -eu

kind=$1
root=$2

packages() {
    apk add --root "$root" --initdb --no-cache --no-scripts --repositories-file /dev/null \
        --repository https://packages.wolfi.dev/os --keys-dir /etc/apk/keys \
        wolfi-baselayout glibc ca-certificates-bundle "$@"
}

case $kind in
builder)
    # AWS-LC's FIPS module refuses to configure without cmake, Go and perl. clang is zstd-sys's bindgen. krb5 and
    # Cyrus SASL want diffutils. build.rs reads the version with git. curl fetches rustup.
    packages bash busybox build-base linux-headers pkgconf cmake go perl clang-22 curl curl-dev git diffutils
    # sh, sed, grep and the rest are busybox applets, whose links Wolfi's package trigger would make.
    chroot "$root" /usr/bin/busybox --install -s
    ;;
image)
    # The binary links only glibc and the libgcc glibc brings. The leader serves, packs and pushes its repository
    # in-process, and its cryptography is inside it.
    packages
    # The bundle's digest as the package installed it, which the STIG's trust-store check compares the bundle to.
    (cd "$root/etc/ssl/certs" && sha256sum ca-certificates.crt >.ca-certificates.crt.sha256)
    # A fixed number, not a name: a kubelet enforcing runAsNonRoot cannot read this image's passwd file. A pod
    # mounting a volume here needs `fsGroup: 1000`.
    echo 'smeltr:x:1000:1000:smeltr:/var/lib/smeltr:/sbin/nologin' >>"$root/etc/passwd"
    echo 'smeltr:x:1000:' >>"$root/etc/group"
    install -d -o 1000 -g 1000 -m 0700 "$root/var/lib/smeltr"
    # Nothing in the image can gain privilege.
    test -z "$(find "$root" -xdev -perm /6000 -type f)"
    ;;
*)
    echo "usage: wolfi-root.sh builder|image <root>" >&2
    exit 2
    ;;
esac
