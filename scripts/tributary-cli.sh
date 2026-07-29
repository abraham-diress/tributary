#!/usr/bin/env bash
# Usage: scripts/tributary-cli.sh <create-topic|describe|produce|consume> [options]
cd "$(dirname "$0")/.."
exec java -cp target/tributary-1.0-SNAPSHOT.jar com.tributary.tools.Cli "$@"
