#!/usr/bin/env bash
set -euo pipefail
if [ "$(timedatectl show --property=NTPSynchronized --value)" != yes ]; then
  echo 'Paper clock is not synchronized; refusing to arm.'
  exit 1
fi
