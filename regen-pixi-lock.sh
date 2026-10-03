#!/bin/bash

# Regenerates pixi.lock in lock file format v6.
#
# KNIME's 5.8 bundler (knime-extension-bundling 5.8.*) pins
# pixi 0.47.0, which can't read lock file format v7 (written by
# pixi >= 0.68). Newer pixi versions have no option to write v6,
# so this script runs pixi 0.67.2, the last release that writes v6.
# Don't bump it.
#
# pixi.lock is deleted first, so all dependencies are re-resolved to
# their newest allowed versions.
#
# The separate cache avoids an 'unexpected end of file' error caused
# by cache data from newer pixi versions.
#
# Run from the directoy containing pixi.toml.

set -e

if [[ ! -f 'pixi.toml' ]]; then
  echo 'There is no pixi.toml file in the current dir.'
  echo 'Make sure you are in the correct dir. Exiting ...'
  exit 1
fi

cache_dir="$(mktemp -d)"
trap 'rm -rf "$cache_dir"' EXIT

rm -rf 'pixi.lock'

PIXI_CACHE_DIR="$cache_dir" \
  pixi exec --channel 'conda-forge' --spec 'pixi==0.67.2' pixi lock

echo 'Successfully generated pixi.lock'
