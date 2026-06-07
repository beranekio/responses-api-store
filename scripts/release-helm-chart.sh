#!/usr/bin/env bash
set -euo pipefail

charts_dir="${CHARTS_DIR:-charts}"
owner="${GITHUB_REPOSITORY_OWNER:?}"
repo="${GITHUB_REPOSITORY#*/}"
config="${CR_CONFIG:-.cr.yaml}"

ensure_local_pages_ref() {
  local remote="${CR_GIT_REMOTE:-origin}"
  local branch="${CR_PAGES_BRANCH:-gh-pages}"
  local ref="refs/remotes/${remote}/${branch}"

  if git show-ref --verify --quiet "$ref"; then
    return 0
  fi

  # chart-releaser v1.7.0 always opens a worktree at remote/branch for cr upload
  # and cr index, even when we publish the index via GitHub Actions. Create a
  # local-only stub ref so git worktree add succeeds without a remote branch.
  echo "Creating local ${remote}/${branch} stub for chart-releaser worktree..."
  local empty_tree commit
  empty_tree="$(git hash-object -t tree /dev/null)"
  commit="$(git commit-tree "$empty_tree" -m "Local stub for chart-releaser worktree")"
  git update-ref "$ref" "$commit"
}

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

ensure_local_pages_ref

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