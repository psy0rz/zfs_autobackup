ZFS autobackup v3 - TEST VERSION
===================================


Official releases are here: https://github.com/psy0rz/zfs_autobackup/releases

New in v3:
 * Complete rewrite, cleaner object oriented code.
 * Python 3 and 2 support.
 * Backwards compatible with your current backups and parameters.
 * Progressive thinning (via a destroy schedule. default schedule should be fine for most people)
 * Cleaner output, with optional color support (pip install colorama). 
   * Clear distinction between local and remote output.
   * Summary at the beginning, displaying what will happen and the current thinning-schedule.
 * More effient destroying/skipping snaphots on the fly. (no more space issues if your backup is way behind)
 * Progress indicator (--progress)
 * Better property management (--set-properties and --filter-properties)
 * Better resume handling, automaticly abort invalid resumes.
 * More robust error handling.
 * Prepared for future enhanchements.
 * Supports raw backups for encryption.