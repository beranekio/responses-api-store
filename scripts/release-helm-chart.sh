#!/usr/bin/env bash
set -euo pipefail

charts_dir="${CHARTS_DIR:-charts}"
owner="${GITHUB_REPOSITORY_OWNER:?}"
repo="${GITHUB_REPOSITORY#*/}"
config="${CR_CONFIG:-.cr.yaml}"

git fetch --tags >/dev/null 2>&1 || true

latest_tag="$(
  git describe --tags --abbrev=0 HEAD~ 2>/dev/null \
    || git rev-list --max-parents=0 --first-parent HEAD
)"

changed_files="$(git diff --find-renames --name-only "$latest_tag" -- "$charts_dir" || true)"
if [[ -z "$changed_files" ]]; then
  echo "No chart changes since ${latest_tag}"
  exit 0
fi

depth=$(($(tr "/" "\n" <<<"$charts_dir" | sed '/^\(\.\)*$/d' | wc -l) + 1))
fields="1-${depth}"

mapfile -t changed_charts < <(
  cut -d '/' -f "$fields" <<<"$changed_files" | uniq | while read -r chart; do
    if [[ -d "$chart" && -f "$chart/Chart.yaml" ]]; then
      echo "$chart"
    fi
  done
)

if [[ ${#changed_charts[@]} -eq 0 ]]; then
  echo "No valid chart changes detected"
  exit 0
fi

rm -rf .cr-release-packages pages
mkdir -p .cr-release-packages pages

for chart in "${changed_charts[@]}"; do
  echo "Packaging ${chart}..."
  cr package "$chart" --package-path .cr-release-packages --config "$config"
done

echo "Uploading releases..."
cr upload -o "$owner" -r "$repo" -c "$(git rev-parse HEAD)" --config "$config" --skip-existing

echo "Generating index for GitHub Pages artifact..."
cr index -o "$owner" -r "$repo" --config "$config" --index-path pages/index.yaml

touch pages/.nojekyll
echo "Prepared pages artifact with charts: ${changed_charts[*]}"