#!/usr/bin/env bash
# Prints the next release tag for the repository in the current directory.
#
#   next-tag.sh [minor|patch]
#
# The latest tag is the highest vX.Y.Z by version sort (pre-release or
# otherwise decorated tags are ignored); v0.0.0 when there is none. "minor"
# (the default) gives vX.(Y+1).0, "patch" gives vX.Y.(Z+1). See
# docs/adr/0001-ci-and-release-tagging.md.
set -euo pipefail

bump="${1:-minor}"

latest="$(git tag --list 'v*.*.*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1 || true)"
latest="${latest:-v0.0.0}"

IFS=. read -r major minor patch <<<"${latest#v}"

case "$bump" in
  minor) echo "v${major}.$((minor + 1)).0" ;;
  patch) echo "v${major}.${minor}.$((patch + 1))" ;;
  *) echo "unknown bump '$bump' (want minor or patch)" >&2; exit 2 ;;
esac
