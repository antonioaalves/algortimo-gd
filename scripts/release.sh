#!/usr/bin/env bash
#
# Release pipeline: bump version -> tag -> push -> build image -> push to OCIR.
#
# Usage:
#   scripts/release.sh patch                 # 1.1.0 -> 1.1.1
#   scripts/release.sh minor                 # 1.1.0 -> 1.2.0
#   scripts/release.sh major                 # 1.1.0 -> 2.0.0
#   scripts/release.sh --version=1.2.3       # set exact version
#   scripts/release.sh patch --skip-push     # local only, no remote push
#   scripts/release.sh patch --skip-registry # tag only, no OCIR push
#   scripts/release.sh patch --skip-release  # no `gh release create`
#
# Environment variables (read from .env if present):
#   OCI_REGION               region key (e.g. fra, iad, lhr)
#   OCI_TENANCY_NAMESPACE    object-storage namespace of the tenancy
#   OCIR_USERNAME            IAM user (typically "tenancy/user@example.com"
#                             or "<tenancy>/<federated-user>"); the tenancy
#                             prefix is prepended automatically if missing.
#   OCIR_AUTH_TOKEN          Oracle Cloud auth token (not the console password)
#   OCIR_REPO                repository name under the tenancy (default: algoritmo-gd)
#
# Requirements: bash, git, docker, gh (optional, for GitHub release).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VERSION_FILE="${REPO_ROOT}/VERSION"

cd "${REPO_ROOT}"

# ---- load .env if present (does not override already-exported vars) --------
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -o allexport
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +o allexport
fi

# ---- arg parsing -----------------------------------------------------------
BUMP=""
EXPLICIT_VERSION=""
SKIP_PUSH=false
SKIP_REGISTRY=false
SKIP_RELEASE=false

for arg in "$@"; do
  case "$arg" in
    major|minor|patch) BUMP="$arg" ;;
    --version=*) EXPLICIT_VERSION="${arg#--version=}" ;;
    --skip-push) SKIP_PUSH=true ;;
    --skip-registry) SKIP_REGISTRY=true ;;
    --skip-release) SKIP_RELEASE=true ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if [[ -z "$BUMP" && -z "$EXPLICIT_VERSION" ]]; then
  echo "Specify a bump (major|minor|patch) or --version=X.Y.Z" >&2
  exit 2
fi

# ---- preconditions ---------------------------------------------------------
if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is dirty. Commit or stash before releasing." >&2
  exit 1
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: ${CURRENT_BRANCH}"

if [[ ! -f "$VERSION_FILE" ]]; then
  echo "0.0.0" > "$VERSION_FILE"
fi
CURRENT_VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")
echo "Current version: ${CURRENT_VERSION}"

# ---- compute new version ---------------------------------------------------
if [[ -n "$EXPLICIT_VERSION" ]]; then
  NEW_VERSION="$EXPLICIT_VERSION"
else
  IFS='.' read -r MAJOR MINOR PATCH <<<"${CURRENT_VERSION%%-*}"
  MAJOR=${MAJOR:-0}; MINOR=${MINOR:-0}; PATCH=${PATCH:-0}
  case "$BUMP" in
    major) MAJOR=$((MAJOR+1)); MINOR=0; PATCH=0 ;;
    minor) MINOR=$((MINOR+1)); PATCH=0 ;;
    patch) PATCH=$((PATCH+1)) ;;
  esac
  NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
fi

if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Invalid semver: ${NEW_VERSION}" >&2
  exit 2
fi

TAG="v${NEW_VERSION}"
if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "Tag ${TAG} already exists." >&2
  exit 1
fi

echo "New version: ${NEW_VERSION}"
echo "New tag:     ${TAG}"

# ---- bump + commit + tag ---------------------------------------------------
echo "${NEW_VERSION}" > "$VERSION_FILE"
git add "$VERSION_FILE"
git commit -m "Release ${TAG}"
git tag -a "${TAG}" -m "Release ${TAG}"

if [[ "$SKIP_PUSH" == false ]]; then
  echo "Pushing ${CURRENT_BRANCH} and ${TAG} to origin..."
  git push origin "${CURRENT_BRANCH}"
  git push origin "${TAG}"
fi

# ---- GitHub release (optional) --------------------------------------------
if [[ "$SKIP_PUSH" == false && "$SKIP_RELEASE" == false ]]; then
  if command -v gh >/dev/null 2>&1; then
    echo "Creating GitHub release ${TAG}..."
    gh release create "${TAG}" --generate-notes --title "${TAG}" || \
      echo "gh release create failed (non-fatal)." >&2
  else
    echo "gh CLI not found; skipping GitHub release." >&2
  fi
fi

# ---- docker build ----------------------------------------------------------
: "${OCIR_REPO:=algoritmo-gd}"
LOCAL_IMAGE="${OCIR_REPO}:${NEW_VERSION}"
LOCAL_LATEST="${OCIR_REPO}:latest"

echo "Building Docker image ${LOCAL_IMAGE}..."
docker build -t "${LOCAL_IMAGE}" -t "${LOCAL_LATEST}" "${REPO_ROOT}"

if [[ "$SKIP_REGISTRY" == true ]]; then
  echo "Skipping OCIR push (--skip-registry)."
  echo "Release ${TAG} complete (local only)."
  exit 0
fi

# ---- OCIR push -------------------------------------------------------------
# Region and tenancy namespace are fixed to the tlanticwfm tenancy in
# Frankfurt (eu-frankfurt-1), which hosts the algoritmo-gd repo:
#   ocid1.containerrepo.oc1.eu-frankfurt-1.0.frsxihvekg43.aaaaaaaa...
: "${OCI_REGION:=fra}"
: "${OCI_TENANCY_NAMESPACE:=frsxihvekg43}"
: "${OCIR_USERNAME:?OCIR_USERNAME is required (IAM user, e.g. user@tlantic.com)}"
: "${OCIR_AUTH_TOKEN:?OCIR_AUTH_TOKEN is required (OCI Console > User Settings > Auth Tokens)}"

REGISTRY="${OCI_REGION}.ocir.io"
# OCIR wants "<tenancy-namespace>/<username>" as the docker login user; add
# the namespace prefix only if the caller hasn't done it already.
if [[ "$OCIR_USERNAME" == */* ]]; then
  DOCKER_USER="$OCIR_USERNAME"
else
  DOCKER_USER="${OCI_TENANCY_NAMESPACE}/${OCIR_USERNAME}"
fi

REMOTE_IMAGE="${REGISTRY}/${OCI_TENANCY_NAMESPACE}/${OCIR_REPO}:${NEW_VERSION}"
REMOTE_LATEST="${REGISTRY}/${OCI_TENANCY_NAMESPACE}/${OCIR_REPO}:latest"

echo "Logging in to ${REGISTRY} as ${DOCKER_USER}..."
printf '%s' "$OCIR_AUTH_TOKEN" | docker login "${REGISTRY}" -u "${DOCKER_USER}" --password-stdin

echo "Tagging and pushing ${REMOTE_IMAGE}..."
docker tag "${LOCAL_IMAGE}" "${REMOTE_IMAGE}"
docker tag "${LOCAL_LATEST}" "${REMOTE_LATEST}"
docker push "${REMOTE_IMAGE}"
docker push "${REMOTE_LATEST}"

echo "Release ${TAG} complete: ${REMOTE_IMAGE}"
