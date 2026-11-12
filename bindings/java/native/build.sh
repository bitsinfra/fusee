#!/bin/sh
# Builds libfusee_jni, the JNI layer of the Java SDK, statically linked with
# libfusee_c.a (cargo build --release -p fusee-c), into
#
#   <out>/dev/fusee/native/<platform>/libfusee_jni.<dylib|so>
#
# where the SDK's loader finds it on the class path (and inside the jar).
# <platform> is darwin-arm64, darwin-x86_64, linux-x86_64 or linux-aarch64.
#
#   native/build.sh [out]        default out: target/classes
#
# Environment: JAVA_HOME (default: the javac on the path), CC (default cc),
# FUSEE_LIB_DIR (default: the repository's target/release, or
# target/<triple>/release when cross-compiling), FUSEE_PLATFORM (override the
# platform directory name, e.g. for a cross-compiled library).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
java_dir=$(cd "$here/.." && pwd)
root=$(cd "$here/../../.." && pwd)
out=${1:-$java_dir/target/classes}
lib_dir=${FUSEE_LIB_DIR:-$root/target/release}

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$arch" in amd64) arch=x86_64 ;; esac
case "$os" in
darwin)
    ext=dylib
    jni_os=darwin
    # Export the JNI entry points and the C interface only.
    shared="-dynamiclib -install_name @rpath/libfusee_jni.dylib -Wl,-dead_strip -Wl,-S -Wl,-w
        -Wl,-exported_symbol,_Java_dev_fusee_Native_* -Wl,-exported_symbol,_JNI_OnLoad -Wl,-exported_symbol,_fusee_*"
    libs="-liconv -lm"
    ;;
linux)
    ext=so
    jni_os=linux
    shared="-shared -Wl,--gc-sections -Wl,--strip-debug -Wl,--exclude-libs,ALL -Wl,-z,noexecstack"
    libs="-lgcc_s -lutil -lrt -lpthread -lm -ldl"
    ;;
*)
    echo "build.sh: unsupported OS $os" >&2
    exit 1
    ;;
esac
platform=${FUSEE_PLATFORM:-$os-$arch}

if [ -z "${JAVA_HOME:-}" ]; then
    javac_path=$(command -v javac) || {
        echo "build.sh: set JAVA_HOME or put javac on the path" >&2
        exit 1
    }
    JAVA_HOME=$(cd "$(dirname "$(readlink -f "$javac_path" 2>/dev/null || echo "$javac_path")")/.." && pwd)
fi
# Maven passes the JRE's java.home; the headers live beside it in old JDKs.
[ -f "$JAVA_HOME/include/jni.h" ] || { [ -f "$JAVA_HOME/../include/jni.h" ] && JAVA_HOME=$(cd "$JAVA_HOME/.." && pwd); }
[ -f "$JAVA_HOME/include/jni.h" ] || {
    echo "build.sh: no jni.h under $JAVA_HOME/include" >&2
    exit 1
}

static_lib=$lib_dir/libfusee_c.a
[ -f "$static_lib" ] || {
    echo "build.sh: $static_lib is missing; run: cargo build --release -p fusee-c" >&2
    exit 1
}

dest=$out/dev/fusee/native/$platform
target=$dest/libfusee_jni.$ext
native_java=$java_dir/src/main/java/dev/fusee/Native.java
if [ -f "$target" ] && [ "$target" -nt "$static_lib" ] && [ "$target" -nt "$here/fusee_jni.c" ] &&
    [ "$target" -nt "$native_java" ] && [ "$target" -nt "$here/build.sh" ] &&
    [ "$target" -nt "$root/bindings/c/include/fusee.h" ]; then
    exit 0
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/fusee-jni.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
# The header javac generates from Native.java: a C signature that does not
# match a native method's fails the compile below.
"$JAVA_HOME/bin/javac" -h "$tmp" -d "$tmp/classes" -implicit:none -sourcepath "$java_dir/src/main/java" "$native_java"

mkdir -p "$dest"
set -f # $shared holds linker patterns, not file globs
# Linking into a fresh name then moving keeps a loaded library's file intact
# (on macOS a library overwritten in place crashes the process using it).
${CC:-cc} -O2 -fPIC -fvisibility=hidden -std=gnu11 -Wall -Wextra -Wno-unused-parameter \
    -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/$jni_os" -I"$root/bindings/c/include" -I"$tmp" \
    "$here/fusee_jni.c" "$static_lib" $shared $libs -o "$tmp/libfusee_jni.$ext"
rm -f "$target"
mv "$tmp/libfusee_jni.$ext" "$target"
echo "built $target"
