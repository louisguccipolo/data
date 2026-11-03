#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
data_dir="$repo_dir/data"
OLIST_COMMIT="d9e49802f3e92d09ee94ab9ccc5e457f207a8959"
base_url="https://raw.githubusercontent.com/olist/work-at-olist-data/$OLIST_COMMIT/datasets"
manifest="$repo_dir/scripts/olist.sha256"

if command -v sha256sum >/dev/null 2>&1; then
    hash_command=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
    hash_command=(shasum -a 256)
else
    echo "Ошибка: нужен sha256sum или shasum" >&2
    exit 1
fi

file_sha256() {
    local checksum
    checksum="$("${hash_command[@]}" -- "$1")" || return
    echo "${checksum%% *}"
}

mkdir -p -- "$data_dir"
tmp_file=""
trap 'if [[ -n "$tmp_file" ]]; then rm -f -- "$tmp_file"; fi' EXIT

while read -r expected file extra; do
    if [[ -z "$expected" || "$expected" == \#* ]]; then
        continue
    fi
    if [[ ! "$expected" =~ ^[0-9a-f]{64}$ || -z "$file" || -n "$extra" ]]; then
        echo "Ошибка: неверная строка в $manifest" >&2
        exit 1
    fi
    if [[ -f "$data_dir/$file" ]]; then
        actual="$(file_sha256 "$data_dir/$file")"
        if [[ "$actual" == "$expected" ]]; then
            echo "$file: уже скачан, sha256 ок"
            continue
        fi
        echo "$file: не та версия (не GitHub Olist), перекачиваю"
    else
        echo "$file: скачивание"
    fi
    tmp_file="$(mktemp "$data_dir/.$file.XXXXXX")"
    curl -fsSL --retry 3 "$base_url/$file" -o "$tmp_file"
    actual="$(file_sha256 "$tmp_file")"
    if [[ "$actual" != "$expected" ]]; then
        echo "Ошибка: $file: sha256 ожидался $expected, получен $actual" >&2
        exit 1
    fi
    mv -- "$tmp_file" "$data_dir/$file"
    tmp_file=""
done < "$manifest"
