//! File format upgrader — detects old format versions and upgrades to current.
//!
//! Supports chain upgrades: v1 → v2 → ... → current.
//! Each upgrade step preserves data integrity and creates a backup.

use std::path::{Path, PathBuf};

use crate::error::GqliteError;
use crate::storage::format::{FORMAT_VERSION, MIN_FORMAT_VERSION};
use crate::storage::pager::Pager;

/// Check the format version of a database file without opening it fully.
///
/// Returns `None` if the file doesn't exist.
/// Returns `Some(version)` if the file exists and has a valid header.
pub fn detect_format_version(path: &Path) -> Result<Option<u32>, GqliteError> {
    if !path.exists() {
        return Ok(None);
    }
    let pager = Pager::open(path)?;
    Ok(Some(pager.header().version))
}

/// Check whether a database file needs upgrading to the current format version.
pub fn needs_upgrade(path: &Path) -> Result<bool, GqliteError> {
    match detect_format_version(path)? {
        None => Ok(false),
        Some(v) => Ok(v < FORMAT_VERSION),
    }
}

/// Upgrade a database file to the current format version.
///
/// Creates a backup at `<path>.v<old_version>.bak` before upgrading.
/// Returns `Ok(true)` if an upgrade was performed, `Ok(false)` if already current.
pub fn upgrade_if_needed(path: &Path) -> Result<bool, GqliteError> {
    let version = match detect_format_version(path)? {
        None => return Ok(false),
        Some(v) => v,
    };

    if version >= FORMAT_VERSION {
        return Ok(false);
    }

    if version < MIN_FORMAT_VERSION {
        return Err(GqliteError::Storage(format!(
            "format version {} is too old to upgrade (minimum: {})",
            version, MIN_FORMAT_VERSION
        )));
    }

    // Create backup
    let backup_path = backup_path_for(path, version);
    std::fs::copy(path, &backup_path).map_err(|e| {
        GqliteError::Storage(format!(
            "failed to create backup at '{}': {}",
            backup_path.display(),
            e
        ))
    })?;

    // Also backup WAL if it exists
    let wal_path = path.with_extension("graph.wal");
    if wal_path.exists() {
        let wal_backup = backup_path.with_extension(format!("v{}.wal.bak", version));
        let _ = std::fs::copy(&wal_path, &wal_backup);
    }

    // Perform step-by-step upgrade
    let mut current_version = version;
    while current_version < FORMAT_VERSION {
        match current_version {
            1 => upgrade_v1_to_v2(path)?,
            _ => {
                return Err(GqliteError::Storage(format!(
                    "no upgrade path from version {} to {}",
                    current_version,
                    current_version + 1
                )));
            }
        }
        current_version += 1;
    }

    Ok(true)
}

/// Generate backup file path for a given version.
fn backup_path_for(path: &Path, version: u32) -> PathBuf {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "database".to_string());
    let ext = path
        .extension()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "graph".to_string());
    path.with_file_name(format!("{}.v{}.bak.{}", stem, version, ext))
}

/// Upgrade from format version 1 to version 2.
///
/// v1 → v2 changes:
/// - Updates format_version in FileHeader from 1 to 2
/// - The data layout remains the same (v1 bincode serialization is still supported)
/// - v2 pages will use page-level checksums when written
///
/// Note: Full page-level storage restructuring (Catalog/NodeTable/RelTable splitting)
/// will be done by tasks 013-015. This upgrade only bumps the version number and
/// ensures the file can be opened by v2 code.
fn upgrade_v1_to_v2(path: &Path) -> Result<(), GqliteError> {
    let mut pager = Pager::open(path)?;

    // Simply update the version number
    {
        let header = pager.header_mut();
        header.version = 2;
    }
    pager.flush_header()?;
    pager.sync()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn temp_path(name: &str) -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join("gqlite_upgrader_test").join(format!(
            "{}_{}_{}",
            name,
            std::process::id(),
            id
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("test.graph")
    }

    fn cleanup(path: &Path) {
        // Remove the entire test directory
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn detect_version_nonexistent() {
        let path = temp_path("nonexistent_detect");
        // temp_path creates a unique dir, file doesn't exist yet
        let _ = std::fs::remove_file(&path); // ensure file doesn't exist
        assert_eq!(detect_format_version(&path).unwrap(), None);
        cleanup(&path);
    }

    #[test]
    fn detect_version_current() {
        let path = temp_path("detect_current");
        Pager::create(&path).unwrap();
        assert_eq!(detect_format_version(&path).unwrap(), Some(FORMAT_VERSION));
        cleanup(&path);
    }

    #[test]
    fn needs_upgrade_false_for_current() {
        let path = temp_path("no_upgrade_needed");
        Pager::create(&path).unwrap();
        assert!(!needs_upgrade(&path).unwrap());
        cleanup(&path);
    }

    #[test]
    fn upgrade_v1_file() {
        let path = temp_path("upgrade_v1");

        // Create a v1 file by hand
        {
            let mut pager = Pager::create(&path).unwrap();
            {
                let header = pager.header_mut();
                header.version = 1;
            }
            pager.flush_header().unwrap();
        }

        // Should need upgrade
        assert!(needs_upgrade(&path).unwrap());

        // Perform upgrade
        let upgraded = upgrade_if_needed(&path).unwrap();
        assert!(upgraded);

        // Verify version is now current
        assert_eq!(detect_format_version(&path).unwrap(), Some(FORMAT_VERSION));

        // Backup should exist
        let backup = backup_path_for(&path, 1);
        assert!(backup.exists(), "backup file should exist: {}", backup.display());

        // No longer needs upgrade
        assert!(!needs_upgrade(&path).unwrap());

        cleanup(&path);
    }

    #[test]
    fn upgrade_idempotent() {
        let path = temp_path("upgrade_idempotent");
        Pager::create(&path).unwrap();

        // Already at current version — no upgrade
        let upgraded = upgrade_if_needed(&path).unwrap();
        assert!(!upgraded);

        cleanup(&path);
    }
}
