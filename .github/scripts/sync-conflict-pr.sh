#!/usr/bin/env bash
# Opens or updates a pull request for resolving upstream merge conflicts in GitHub's web editor.
#
# The PR head is the upstream commit plus one bot commit that sets fork-owned paths, generated
# lockfiles and modify/delete conflicts to the fork's version. That leaves only real text
# conflicts, which GitHub's "Resolve conflicts" editor can handle by merging the fork branch in.
#
# Required environment:
#   FORK_BRANCH     branch the PR targets, e.g. master
#   UPSTREAM_REF    already fetched upstream ref, e.g. upstream/master
#   UPSTREAM_NAME   label used in the PR text, e.g. Jackett/Jackett@master
#   GH_TOKEN        token for gh and for pushing; a PAT (SYNC_TOKEN) so pushes may touch workflows and trigger CI
#   GH_REPO         owner/name of this fork
# Optional environment:
#   BASE_SHA        fork commit to merge into (default: origin/$FORK_BRANCH)
#   SYNC_BRANCH     PR head branch (default: upstream-sync)
#   KEEP_FORK_PATHS space-separated paths that always keep the fork's version (default: .github/workflows)
#   LOCKFILES       space-separated generated files that sync-conflicts.yml regenerates
#   MERGE_OPTS      extra git merge options, e.g. -Xrenormalize
#   PR_NOTE         extra markdown appended to the PR description
set -euo pipefail

sync_branch="${SYNC_BRANCH:-upstream-sync}"
base="$(git rev-parse --verify "${BASE_SHA:-origin/$FORK_BRANCH}^{commit}")"
upstream="$(git rev-parse --verify "$UPSTREAM_REF^{commit}")"
read -r -a keep <<< "${KEEP_FORK_PATHS-.github/workflows}"
read -r -a lockfiles <<< "${LOCKFILES:-}"
read -r -a merge_opts <<< "${MERGE_OPTS:-}"
bot=(git -c user.name="github-actions[bot]" -c user.email="41898282+github-actions[bot]@users.noreply.github.com")

tmp="$(mktemp -d)"
wt="$tmp/worktree"
cleanup() {
    git worktree remove --force "$wt" >/dev/null 2>&1 || true
    rm -rf "$tmp"
}
trap cleanup EXIT

in_paths() {
    local file="$1" path
    shift
    for path in "$@"; do
        [[ "$file" == "$path" || "$file" == "$path"/* ]] && return 0
    done
    return 1
}

keep_fork_paths() {
    local path
    for path in "${keep[@]}"; do
        git rm -r -q -f --ignore-unmatch -- "$path"
        if git cat-file -e "$base:$path" 2>/dev/null; then
            git checkout "$base" -- "$path"
        fi
    done
}

git worktree add -q --detach "$wt" "$base"
cd "$wt"

"${bot[@]}" merge --no-commit --no-ff "${merge_opts[@]}" "$upstream" >/dev/null 2>&1 || true
mapfile -t conflicts < <(git diff --name-only --diff-filter=U)
# Modify/delete conflicts have only one of the "ours" (2) and "theirs" (3) stages
mapfile -t deleted < <(git ls-files -u | awk -F'\t' '{ split($1, f, " "); s[$2] = s[$2] f[3] } END { for (p in s) if (s[p] !~ /2/ || s[p] !~ /3/) print p }' | sort)

editable=()
fixed=()
for file in "${conflicts[@]}"; do
    if in_paths "$file" "${keep[@]}"; then
        continue
    elif in_paths "$file" "${lockfiles[@]}" "${deleted[@]}"; then
        fixed+=("$file")
    else
        editable+=("$file")
    fi
done
fixed_deleted=()
for file in "${fixed[@]}"; do
    in_paths "$file" "${deleted[@]}" && fixed_deleted+=("$file")
done

if [[ ${#editable[@]} -eq 0 ]]; then
    # Nothing for the web editor: commit the real merge so sync-conflicts.yml can regenerate lockfiles
    keep_fork_paths
    for file in "${fixed[@]}"; do
        if git cat-file -e ":3:$file" 2>/dev/null && ! in_paths "$file" "${deleted[@]}"; then
            git checkout --theirs -- "$file"
            git add -- "$file"
        elif git cat-file -e ":2:$file" 2>/dev/null; then
            git checkout --ours -- "$file"
            git add -- "$file"
        elif git cat-file -e ":3:$file" 2>/dev/null; then
            git checkout --theirs -- "$file"
            git add -- "$file"
        else
            git rm -q -f --ignore-unmatch -- "$file"
        fi
    done
    "${bot[@]}" commit -q -m "Merge $UPSTREAM_NAME into $FORK_BRANCH"
else
    git merge --abort
    git checkout -q --detach "$upstream"
    keep_fork_paths
    for file in "${fixed[@]}"; do
        if git cat-file -e "$base:$file" 2>/dev/null; then
            git checkout "$base" -- "$file"
        else
            git rm -q -f --ignore-unmatch -- "$file"
        fi
    done
    if ! git diff --cached --quiet; then
        "${bot[@]}" commit -q -m "chore(sync): keep $FORK_BRANCH versions of fork-owned files and non-editable conflicts [skip ci]"
    fi
fi
head="$(git rev-parse HEAD)"
cd - >/dev/null

pr="$(gh pr list --head "$sync_branch" --base "$FORK_BRANCH" --state open --json number --jq '.[0].number // empty')"
if git fetch -q origin "$sync_branch" 2>/dev/null; then
    # Only replace the branch while it holds nothing but bot commits
    authors="$(git log --format=%an FETCH_HEAD --not "$upstream" "$base" | sort -u)"
    if [[ -n "$pr" && -n "$authors" && "$authors" != "github-actions[bot]" ]]; then
        echo "::notice::$sync_branch has resolution commits; leaving it untouched until PR #$pr is merged."
        exit 0
    fi
    if [[ -n "$pr" && "$(git rev-parse "FETCH_HEAD^{tree}")" == "$(git rev-parse "$head^{tree}")" ]]; then
        echo "$sync_branch is already up to date."
        exit 0
    fi
fi
# The checkout's stored credentials are cleared so the push uses GH_TOKEN
git -c http.https://github.com/.extraheader= push -q --force \
    "https://x-access-token:${GH_TOKEN}@github.com/${GH_REPO}.git" "$head:refs/heads/$sync_branch"

body="$tmp/body.md"
{
    echo "Automatic sync of \`$UPSTREAM_NAME\` (\`${upstream:0:7}\`) into \`$FORK_BRANCH\` hit merge conflicts."
    echo
    if [[ ${#editable[@]} -gt 0 ]]; then
        echo "### Resolve these conflicts"
        printf -- '- `%s`\n' "${editable[@]}"
        echo
        echo "Click **Resolve conflicts** at the bottom of this page, pick the right code in each file, then **Mark as resolved** and **Commit merge**. That commits the merge to \`$sync_branch\`."
        echo
    else
        echo "Every conflict was in a file GitHub's editor can't handle, so the merge is already done on this branch. Review the changes, then merge."
        echo
    fi
    if [[ ${#fixed_deleted[@]} -gt 0 ]]; then
        echo "### Deleted on one side, changed on the other"
        echo "GitHub's editor can't resolve these, so the surviving version was kept. Check whether each should still exist:"
        printf -- '- `%s`\n' "${fixed_deleted[@]}"
        echo
    fi
    if [[ -n "${PR_NOTE:-}" ]]; then
        echo "$PR_NOTE"
        echo
    fi
    echo "### What happens next"
    if [[ ${#lockfiles[@]} -gt 0 ]]; then
        echo "- After each push here, the **Sync conflicts** check makes sure no conflict markers are left and regenerates $(printf '`%s`, ' "${lockfiles[@]}" | sed 's/, $//')."
    else
        echo "- After each push here, the **Sync conflicts** check makes sure no conflict markers are left."
    fi
    echo "- When the checks pass, merge with **Create a merge commit**. Squash or rebase drops the upstream history, and the next sync would conflict again."
} > "$body"

if [[ -z "$pr" ]]; then
    gh pr create --head "$sync_branch" --base "$FORK_BRANCH" --title "Resolve upstream sync conflicts" --body-file "$body"
else
    gh pr edit "$pr" --body-file "$body"
fi
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat "$body" >> "$GITHUB_STEP_SUMMARY"
fi
