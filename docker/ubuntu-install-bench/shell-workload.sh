#!/usr/bin/env bash
set -euo pipefail

LARGE_FILE_DIR_REL="assets/large"
LARGE_FILE_MANIFEST_REL="${LARGE_FILE_DIR_REL}/bench-files.txt"

write_zero_file_mib() {
    local path="$1"
    local size_mib="$2"
    /usr/bin/dd if=/dev/zero of="$path" bs=1M count="$size_mib" status=none
}

generate_seed_workload() {
    local seed_dir="$1"
    local module_count=36
    local group_count=6
    local asset_dir

    mkdir -p "$seed_dir"
    /usr/bin/find "$seed_dir" -mindepth 1 -maxdepth 1 -exec /bin/rm -rf -- {} +
    mkdir -p \
        "$seed_dir/project/common/config" \
        "$seed_dir/project/modules" \
        "$seed_dir/project/packaging/groups" \
        "$seed_dir/project/${LARGE_FILE_DIR_REL}"

    cat >"$seed_dir/project/common/config/base.env" <<'BASE'
PROJECT_NAME=fro-shell-bench
ENABLE_LOGGING=1
ENABLE_PACKAGING=1
ENABLE_TESTS=1
ENABLE_INSTALL=1
BASE

    local i j module module_dir group_index group_file feature_index
    for i in $(seq 1 "$module_count"); do
        module="module-$(printf '%02d' "$i")"
        module_dir="$seed_dir/project/modules/$module"
        mkdir -p \
            "$module_dir/src" \
            "$module_dir/include" \
            "$module_dir/docs" \
            "$module_dir/tests/cases" \
            "$module_dir/scripts" \
            "$module_dir/config"

        cat >"$module_dir/config/module.env" <<EOF_MODULE
MODULE_NAME=$module
MODULE_INDEX=$i
ENABLE_FAST_PATH=$((i % 2))
ENABLE_SHELL_SLICE=$((i % 3))
ENABLE_ARCHIVE_MODE=$((i % 5))
EOF_MODULE

        : >"$module_dir/config/features.list"
        for j in $(seq 1 12); do
            feature_index=$(((i + j) % 9))
            printf 'ENABLE_FEATURE_%02d=%s\n' "$feature_index" "$((j % 2))" >>"$module_dir/config/features.list"
        done

        cat >"$module_dir/include/${module}.h" <<EOF_HEADER
#ifndef ${module//-/_}_H
#define ${module//-/_}_H
int ${module//-/_}_run(void);
#endif
EOF_HEADER

        cat >"$module_dir/src/main.c" <<EOF_MAIN
#include "${module}.h"
int ${module//-/_}_run(void) {
    return $i;
}
EOF_MAIN

        cat >"$module_dir/src/lib.c" <<EOF_LIB
#include "${module}.h"
int ${module//-/_}_helper(void) {
    return ${i} * 2;
}
EOF_LIB

        {
            printf '%s configure README\n' "$module"
            for j in $(seq 1 72); do
                printf '%s line %03d ENABLE_FEATURE_%02d shell-workload-%02d\n' "$module" "$j" "$(((i + j) % 9))" "$((j % 7))"
            done
        } >"$module_dir/docs/README.txt"

        {
            printf '#!/usr/bin/env bash\n'
            printf 'echo running %s post-install\n' "$module"
            printf 'echo ENABLE_FEATURE_%02d\n' "$((i % 9))"
        } >"$module_dir/scripts/postinst.sh"
        chmod +x "$module_dir/scripts/postinst.sh"

        {
            printf 'case,module,line\n'
            for j in $(seq 1 40); do
                printf 'alpha,%s,%03d\n' "$module" "$j"
            done
        } >"$module_dir/tests/cases/case-1.txt"

        {
            printf 'case,module,line\n'
            for j in $(seq 1 44); do
                printf 'beta,%s,%03d\n' "$module" "$j"
            done
        } >"$module_dir/tests/cases/case-2.txt"
    done

    for i in $(seq 1 "$group_count"); do
        group_file="$seed_dir/project/packaging/groups/bundle-$(printf '%02d' "$i").list"
        : >"$group_file"
    done
    for i in $(seq 1 "$module_count"); do
        module="module-$(printf '%02d' "$i")"
        group_index=$((((i - 1) % group_count) + 1))
        printf '%s\n' "$module" >>"$seed_dir/project/packaging/groups/bundle-$(printf '%02d' "$group_index").list"
    done

    asset_dir="$seed_dir/project/${LARGE_FILE_DIR_REL}"
    cat >"$seed_dir/project/${LARGE_FILE_MANIFEST_REL}" <<'EOF_LARGE'
large-01.bin
large-02.bin
medium-01.bin
medium-02.bin
medium-03.bin
medium-04.bin
medium-05.bin
medium-06.bin
EOF_LARGE
    write_zero_file_mib "$asset_dir/large-01.bin" 4096
    write_zero_file_mib "$asset_dir/large-02.bin" 4096
    write_zero_file_mib "$asset_dir/medium-01.bin" 112
    write_zero_file_mib "$asset_dir/medium-02.bin" 128
    write_zero_file_mib "$asset_dir/medium-03.bin" 144
    write_zero_file_mib "$asset_dir/medium-04.bin" 160
    write_zero_file_mib "$asset_dir/medium-05.bin" 176
    write_zero_file_mib "$asset_dir/medium-06.bin" 192
}

prepare_workspace() {
    local seed_dir="$1"
    local work_dir="$2"
    rm -rf "$work_dir/project" "$work_dir/build" "$work_dir/package" "$work_dir/verify"
    mkdir -p "$work_dir"
    cp -a "$seed_dir/project" "$work_dir/project"
}

run_configure_slice() {
    local project_root="$1"
    local build_root="$2"
    local module_dir module tmp_base

    mkdir -p "$build_root/config" "$build_root/headers" "$build_root/reports" "$build_root/manifests"
    find "$project_root" -type f | sort >"$build_root/manifests/all-files.txt"
    wc -l "$build_root/manifests/all-files.txt" >"$build_root/manifests/all-file-count.txt"
    : >"$build_root/manifests/module-list.txt"
    for module_dir in "$project_root"/modules/*; do
        basename "$module_dir" >>"$build_root/manifests/module-list.txt"
    done
    sort "$build_root/manifests/module-list.txt" >"$build_root/manifests/module-list.tmp"
    mv "$build_root/manifests/module-list.tmp" "$build_root/manifests/module-list.txt"

    while read -r module; do
        module_dir="$project_root/modules/$module"
        tmp_base="$build_root/reports/$module"
        cat \
            "$project_root/common/config/base.env" \
            "$module_dir/config/module.env" \
            "$module_dir/config/features.list" >"$build_root/config/${module}.cfg.tmp"
        fgrep 'ENABLE_' "$build_root/config/${module}.cfg.tmp" | sort >"$build_root/config/${module}.features.tmp"
        head -n 12 "$module_dir/docs/README.txt" >"${tmp_base}.head.tmp"
        tail -n 8 "$module_dir/docs/README.txt" >"${tmp_base}.tail.tmp"
        wc -l "$module_dir/tests/cases/case-1.txt" "$module_dir/tests/cases/case-2.txt" >"${tmp_base}.wc.tmp"
        cp "$module_dir/include/${module}.h" "$build_root/headers/${module}.h.tmp"
        cat \
            "${tmp_base}.head.tmp" \
            "${tmp_base}.tail.tmp" \
            "${tmp_base}.wc.tmp" >"${tmp_base}.summary.tmp"
        mv "$build_root/config/${module}.cfg.tmp" "$build_root/config/${module}.cfg"
        mv "$build_root/config/${module}.features.tmp" "$build_root/config/${module}.features"
        mv "$build_root/headers/${module}.h.tmp" "$build_root/headers/${module}.h"
        mv "${tmp_base}.summary.tmp" "${tmp_base}.summary"
    done <"$build_root/manifests/module-list.txt"
}

run_build_slice() {
    local project_root="$1"
    local build_root="$2"
    local module_dir module bundle_dir checksum_dir size_dir install_dir

    bundle_dir="$build_root/bundles"
    checksum_dir="$build_root/checksums"
    size_dir="$build_root/sizes"
    install_dir="$build_root/install"
    mkdir -p "$bundle_dir" "$checksum_dir" "$size_dir" "$install_dir"

    while read -r module; do
        module_dir="$project_root/modules/$module"
        cat \
            "$build_root/headers/${module}.h" \
            "$build_root/config/${module}.cfg" \
            "$build_root/reports/${module}.summary" \
            "$module_dir/src/main.c" \
            "$module_dir/src/lib.c" \
            "$module_dir/scripts/postinst.sh" >"$bundle_dir/${module}.bundle.tmp"
        cksum "$bundle_dir/${module}.bundle.tmp" >"$checksum_dir/${module}.cksum.tmp"
        wc -c "$bundle_dir/${module}.bundle.tmp" >"$size_dir/${module}.size.tmp"
        cp "$module_dir/scripts/postinst.sh" "$install_dir/${module}.postinst.tmp"
        mv "$bundle_dir/${module}.bundle.tmp" "$bundle_dir/${module}.bundle"
        mv "$checksum_dir/${module}.cksum.tmp" "$checksum_dir/${module}.cksum"
        mv "$size_dir/${module}.size.tmp" "$size_dir/${module}.size"
        mv "$install_dir/${module}.postinst.tmp" "$install_dir/${module}.postinst"
    done <"$build_root/manifests/module-list.txt"

    find "$bundle_dir" -type f | sort >"$build_root/manifests/bundle-files.txt"
}

run_large_io_slice() {
    local project_root="$1"
    local build_root="$2"
    local package_root="$3"
    local asset_root manifest large_build_root file file_name

    asset_root="$project_root/${LARGE_FILE_DIR_REL}"
    manifest="$project_root/${LARGE_FILE_MANIFEST_REL}"
    large_build_root="$build_root/large"
    mkdir -p \
        "$large_build_root/checksums" \
        "$large_build_root/copies" \
        "$package_root/packages" \
        "$package_root/lists" \
        "$package_root/reports"

    cp "$asset_root/large-01.bin" "$large_build_root/copies/large-01.copy.tmp"
    mv "$large_build_root/copies/large-01.copy.tmp" "$large_build_root/copies/large-01.copy"
    cp "$asset_root/medium-03.bin" "$large_build_root/copies/medium-03.copy.tmp"
    mv "$large_build_root/copies/medium-03.copy.tmp" "$large_build_root/copies/medium-03.copy"

    while read -r file_name; do
        file="$asset_root/$file_name"
        cksum "$file" >"$large_build_root/checksums/${file_name}.cksum.tmp"
        mv \
            "$large_build_root/checksums/${file_name}.cksum.tmp" \
            "$large_build_root/checksums/${file_name}.cksum"
    done <"$manifest"

    (
        cd "$project_root/assets"
        tar -cf "$package_root/packages/large-assets.tar" large
    )
    tar -tf "$package_root/packages/large-assets.tar" | sort >"$package_root/lists/large-assets.contents.tmp"
    head -n 10 "$package_root/lists/large-assets.contents.tmp" >"$package_root/reports/large-assets.head.tmp"
    tail -n 10 "$package_root/lists/large-assets.contents.tmp" >"$package_root/reports/large-assets.tail.tmp"
    wc -l "$package_root/lists/large-assets.contents.tmp" >"$package_root/reports/large-assets.count.tmp"
    cat \
        "$package_root/reports/large-assets.head.tmp" \
        "$package_root/reports/large-assets.tail.tmp" \
        "$package_root/reports/large-assets.count.tmp" >"$package_root/reports/large-assets.report.tmp"
    mv "$package_root/lists/large-assets.contents.tmp" "$package_root/lists/large-assets.contents"
    mv "$package_root/reports/large-assets.report.tmp" "$package_root/reports/large-assets.report"
}

run_package_slice() {
    local project_root="$1"
    local build_root="$2"
    local package_root="$3"
    local group_file group stage_dir module

    mkdir -p \
        "$package_root/stage" \
        "$package_root/stage-complete" \
        "$package_root/packages" \
        "$package_root/lists" \
        "$package_root/reports"

    for group_file in "$project_root"/packaging/groups/*.list; do
        group="$(basename "$group_file" .list)"
        stage_dir="$package_root/stage/$group"
        mkdir -p "$stage_dir/bundles" "$stage_dir/checksums" "$stage_dir/reports" "$stage_dir/postinst"
        while read -r module; do
            cp "$build_root/bundles/${module}.bundle" "$stage_dir/bundles/${module}.bundle"
            cp "$build_root/checksums/${module}.cksum" "$stage_dir/checksums/${module}.cksum"
            cp "$build_root/reports/${module}.summary" "$stage_dir/reports/${module}.summary"
            cp "$build_root/install/${module}.postinst" "$stage_dir/postinst/${module}.postinst"
        done <"$group_file"
        (
            cd "$stage_dir"
            tar -cf "$package_root/packages/${group}.tar" .
        )
        tar -tf "$package_root/packages/${group}.tar" | sort >"$package_root/lists/${group}.contents.tmp"
        head -n 10 "$package_root/lists/${group}.contents.tmp" >"$package_root/reports/${group}.head.tmp"
        tail -n 10 "$package_root/lists/${group}.contents.tmp" >"$package_root/reports/${group}.tail.tmp"
        wc -l "$package_root/lists/${group}.contents.tmp" >"$package_root/reports/${group}.count.tmp"
        cat \
            "$package_root/reports/${group}.head.tmp" \
            "$package_root/reports/${group}.tail.tmp" \
            "$package_root/reports/${group}.count.tmp" >"$package_root/reports/${group}.report.tmp"
        mv "$package_root/lists/${group}.contents.tmp" "$package_root/lists/${group}.contents"
        mv "$package_root/reports/${group}.report.tmp" "$package_root/reports/${group}.report"
        mv "$stage_dir" "$package_root/stage-complete/$group"
    done

    find "$package_root/packages" -type f | sort >"$package_root/package-archives.txt"
}

run_verify_slice() {
    local package_root="$1"
    local verify_root="$2"
    local archive archive_name extract_dir

    mkdir -p "$verify_root/contents" "$verify_root/extracted" "$verify_root/reports" "$verify_root/scratch"

    while read -r archive; do
        archive_name="$(basename "$archive" .tar)"
        extract_dir="$verify_root/extracted/$archive_name"
        mkdir -p "$extract_dir"
        tar -xf "$archive" -C "$extract_dir"
        find "$extract_dir" -type f | sort >"$verify_root/contents/${archive_name}.files.tmp"
        wc -l "$verify_root/contents/${archive_name}.files.tmp" >"$verify_root/reports/${archive_name}.count.tmp"
        fgrep '.bundle' "$verify_root/contents/${archive_name}.files.tmp" >"$verify_root/reports/${archive_name}.bundles.tmp"
        head -n 5 "$verify_root/contents/${archive_name}.files.tmp" >"$verify_root/reports/${archive_name}.head.tmp"
        tail -n 5 "$verify_root/contents/${archive_name}.files.tmp" >"$verify_root/reports/${archive_name}.tail.tmp"
        cat \
            "$verify_root/reports/${archive_name}.count.tmp" \
            "$verify_root/reports/${archive_name}.bundles.tmp" \
            "$verify_root/reports/${archive_name}.head.tmp" \
            "$verify_root/reports/${archive_name}.tail.tmp" >"$verify_root/reports/${archive_name}.report.tmp"
        mv "$verify_root/contents/${archive_name}.files.tmp" "$verify_root/contents/${archive_name}.files"
        mv "$verify_root/reports/${archive_name}.report.tmp" "$verify_root/reports/${archive_name}.report"
    done <"$package_root/package-archives.txt"
}

run_cleanup_slice() {
    local work_dir="$1"
    rm -rf "$work_dir/package/stage" "$work_dir/verify/scratch"
}
