use super::*;

pub(super) fn maybe_print_verbose_copy(
    verbose: bool,
    cp_compat: bool,
    source_path: &Path,
    target_path: &Path,
) {
    if verbose && cp_compat {
        fro::cio_println!("'{}' -> '{}'", source_path.display(), target_path.display());
    }
}
