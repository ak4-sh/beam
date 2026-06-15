#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Publish the Beam Sponge Java artifacts needed by Nemo into ~/.m2.

Usage:
  scripts/publish_for_nemo.sh

Environment:
  JAVA_HOME          Prefer a Java 8 JDK for this old Beam/Gradle build.
  GRADLE_ARGS       Extra args passed to ./gradlew.
  PUBLISH_ALL       If true, run root publishToMavenLocal instead of the targeted set.
  SKIP_JAVADOC      If true, pass -x javadoc. Default: true.
  ALLOW_NON_JAVA8   If true, do not warn/fail on non-Java-8 builds. Default: false.

Why this exists:
  Nemo Sponge resolves Beam artifacts by Maven coordinates, for example:
    org.apache.beam:beam-sdks-java-nexmark:2.6.0-SNAPSHOT
    org.apache.beam:beam-sdks-java-io-kafka:2.6.0-SNAPSHOT

  Beam's Gradle build only creates publishToMavenLocal tasks when -Ppublishing is set.
  This wrapper makes that requirement explicit and avoids accidentally publishing release
  coordinates with -PisRelease.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -x ./gradlew || ! -f settings.gradle ]]; then
  echo "Run this script from the Beam repository checkout." >&2
  exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
if [[ "${CURRENT_BRANCH}" != "sponge" ]]; then
  echo "Expected Beam branch 'sponge', found '${CURRENT_BRANCH:-unknown}'." >&2
  exit 1
fi

if [[ "${GRADLE_ARGS:-}" == *"-PisRelease"* ]]; then
  echo "Do not use -PisRelease for Nemo. Nemo expects Beam 2.6.0-SNAPSHOT artifacts." >&2
  exit 1
fi

JAVA_VERSION_OUTPUT="$(java -version 2>&1 | head -n 1 || true)"
if [[ "${ALLOW_NON_JAVA8:-false}" != "true" && "${JAVA_VERSION_OUTPUT}" != *'"1.8.'* ]]; then
  echo "This old Beam/Gradle build is most reliable with Java 8." >&2
  echo "Current java -version: ${JAVA_VERSION_OUTPUT}" >&2
  echo "Set JAVA_HOME to a Java 8 JDK, or rerun with ALLOW_NON_JAVA8=true." >&2
  exit 1
fi

COMMON_ARGS=("-Ppublishing")
if [[ "${SKIP_JAVADOC:-true}" == "true" ]]; then
  COMMON_ARGS+=("-x" "javadoc")
fi

if [[ -n "${GRADLE_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS=(${GRADLE_ARGS})
else
  EXTRA_ARGS=()
fi

if [[ "${PUBLISH_ALL:-false}" == "true" ]]; then
  exec ./gradlew "${COMMON_ARGS[@]}" "${EXTRA_ARGS[@]}" publishToMavenLocal
fi

TARGETS=(
  ":beam-sdks-java-core:publishToMavenLocal"
  ":beam-runners-core-java:publishToMavenLocal"
  ":beam-runners-core-construction-java:publishToMavenLocal"
  ":beam-model-pipeline:publishToMavenLocal"
  ":beam-model-job-management:publishToMavenLocal"
  ":beam-model-fn-execution:publishToMavenLocal"
  ":beam-sdks-java-extensions-protobuf:publishToMavenLocal"
  ":beam-vendor-sdks-java-extensions-protobuf:publishToMavenLocal"
  ":beam-sdks-java-extensions-google-cloud-platform-core:publishToMavenLocal"
  ":beam-sdks-java-io-google-cloud-platform:publishToMavenLocal"
  ":beam-sdks-java-io-hadoop-input-format:publishToMavenLocal"
  ":beam-sdks-java-io-kafka:publishToMavenLocal"
  ":beam-sdks-java-extensions-sql:publishToMavenLocal"
  ":beam-sdks-java-nexmark:publishToMavenLocal"
)

exec ./gradlew "${COMMON_ARGS[@]}" "${EXTRA_ARGS[@]}" "${TARGETS[@]}"
