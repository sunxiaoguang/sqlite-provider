#!/usr/bin/env bash
set -euo pipefail

profile="${1:-debug}"

case "$(uname -s)" in
  Darwin)
    libname="libsqlite_provider_abi.dylib"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    libname="sqlite_provider_abi.dll"
    ;;
  *)
    libname="libsqlite_provider_abi.so"
    ;;
esac

path="$(pwd)/target/${profile}/${libname}"
test -f "${path}"
printf '%s\n' "${path}"
