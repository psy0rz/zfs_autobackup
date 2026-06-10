"""Snapshot-level replication: sync the snapshots of one dataset to a target dataset,
thinning out obsolete snapshots on both sides along the way."""

from .ZfsBookmark import ZfsBookmark
from .ZfsContainer import ZfsContainer
from .ZfsPointInTime import ZfsPointInTime
from .ZfsSnapshot import ZfsSnapshot


def _resolve_clone_origin(source_dataset, target_node, target_path, strip_path, guid_check):
    """Return the source-side origin snapshot to use as zfs send -i base for a clone,
    or None if the clone relationship cannot be preserved on the target.

    :type source_dataset: ZfsContainer
    :type target_node: ZfsNode
    :type target_path: str
    :type strip_path: int
    :type guid_check: bool
    :rtype: ZfsSnapshot|None
    """

    source_origin_snap = source_dataset.origin
    if source_origin_snap is None:
        return None

    # Reverse-clone topology (after 'zfs promote' of a child): origin lives on a namespace descendant
    # of this dataset. zfs recv cannot land a clone-creating stream on top of the placeholder that has
    # to exist for the descendant, so replication can't preserve the relationship.
    origin_parent_path = source_origin_snap.name.split('@', 1)[0]
    if origin_parent_path == source_dataset.name or origin_parent_path.startswith(source_dataset.name + "/"):
        source_dataset.warning(
            "Cannot replicate as clone: origin '{}' lives on a namespace descendant of this dataset. "
            "Falling back to full send.".format(source_origin_snap.name))
        return None

    try:
        target_origin_parent = source_origin_snap.parent.map_to_target_path(target_path, strip_path)
    except Exception as e:
        source_dataset.warning(
            "Cannot replicate as clone: cannot map origin '{}' to target ({}). "
            "Falling back to full send.".format(source_origin_snap.name, str(e)))
        return None

    target_origin_snap = target_node.get_snapshot(target_origin_parent + '@' + source_origin_snap.suffix)

    if not target_origin_snap.exists:
        source_dataset.warning(
            "Cannot replicate as clone: origin '{}' not available on target. "
            "Falling back to full send.".format(source_origin_snap.name))
        return None

    if guid_check and not source_origin_snap.guid_matches(target_origin_snap):
        source_dataset.warning(
            "Cannot replicate as clone: origin guid mismatch between source and target. "
            "Falling back to full send.")
        return None

    return source_origin_snap


def _pre_clean(source_dataset, target_dataset, source_common_snapshot, source_obsoletes, target_obsoletes,
               target_transfers):
    """cleanup old stuff on source and target before starting snapshot syncing

    :type source_dataset: ZfsContainer
    :type target_dataset: ZfsContainer
    :type source_common_snapshot: ZfsSnapshot|ZfsBookmark|None
    :type source_obsoletes: list[ZfsSnapshot]
    :type target_obsoletes: list[ZfsSnapshot]
    :type target_transfers: list[ZfsSnapshot]
    """

    # on source: delete all obsoletes that are not in target_transfers (except common snapshot, if its not a bookmark)
    for source_snapshot in source_dataset.snapshots:
        if (source_snapshot in source_obsoletes
                and source_common_snapshot != source_snapshot
                and source_snapshot.find_snapshot_by_suffix(target_transfers) is None):
            source_snapshot.destroy()

    # on target: destroy everything thats obsolete, except the common snapshot
    if not target_dataset.exists:
        return

    for target_snapshot in target_dataset.snapshots:
        if (target_snapshot in target_obsoletes
                and (not source_common_snapshot
                     or target_snapshot.tagless_suffix != source_common_snapshot.tagless_suffix)
                and target_snapshot.exists):
            target_snapshot.destroy()


def _handle_incompatible_target(target_dataset, incompatible_target_snapshots, destroy_incompatible, force,
                                source_common_snapshot, is_resume):
    """destroy incompatible snapshots on target before sync, or inform user what to do

    :type target_dataset: ZfsContainer
    :type incompatible_target_snapshots: list[ZfsSnapshot]
    :type destroy_incompatible: bool
    :type force: bool
    :type source_common_snapshot: ZfsPointInTime|None
    :type is_resume: bool
    """

    if not target_dataset.exists:
        # no target yet, so everything ok
        return

    if not incompatible_target_snapshots and source_common_snapshot is not None:
        # nice existing target with compatible common snapshot.
        return

    if not incompatible_target_snapshots and is_resume and len(target_dataset.snapshots) == 0:
        # its a dataset from an incremental send we probably can resume, so its fine
        return

    # from this point on things get progressively worse..

    if source_common_snapshot:
        # we just need to delete some incompatibles:
        for snapshot in incompatible_target_snapshots:
            snapshot.warning("Incompatible snapshot")

        if not destroy_incompatible:
            raise Exception("Use --destroy-incompatible to get rid of these.")
    else:
        # no common snapshot, so the whole dataset is incompatible
        if incompatible_target_snapshots:
            if not force or not destroy_incompatible:
                target_dataset.error("Incompatible dataset!")
                raise Exception("Use --destroy-incompatible -F to overwrite the target dataset and start over.")
        elif not force:
            target_dataset.error("Incompatible dataset!")
            raise Exception("Use -F to overwrite the target dataset and start over.")

        target_dataset.warning("Overwriting incompatible dataset.")

    # remove incompatibles and rollback
    for snapshot in incompatible_target_snapshots:
        snapshot.destroy(fail_exception=True)

    target_dataset.rollback()


def _validate_resume_token(source_dataset, target_dataset, start_snapshot):
    """validate and return the resume token, or abort (destroy) the resume state if its no longer valid

    :type source_dataset: ZfsContainer
    :type target_dataset: ZfsContainer
    :type start_snapshot: ZfsSnapshot|None
    :rtype: str|None
    """

    if not target_dataset.exists or 'receive_resume_token' not in target_dataset.properties:
        return None

    if start_snapshot is None:
        target_dataset.verbose("Aborting resume, its obsolete.")
        target_dataset.abort_resume()
        return None

    resume_token = target_dataset.properties['receive_resume_token']

    # not valid anymore?
    resume_snapshot = source_dataset.zfs_node.get_resume_snapshot(resume_token)
    if not resume_snapshot or start_snapshot.suffix != resume_snapshot.suffix:
        target_dataset.verbose("Aborting resume, its no longer valid.")
        target_dataset.abort_resume()
        return None

    return resume_token


def _plan_sync(source_dataset, target_dataset, also_other_snapshots, guid_check, raw, bookmark_tag,
               required_snapshots=None):
    """Determine at what snapshot to start syncing to target_dataset and what to sync and what to keep.

    :type source_dataset: ZfsContainer
    :type target_dataset: ZfsContainer
    :type also_other_snapshots: bool
    :type guid_check: bool
    :type raw: bool
    :type bookmark_tag: str
    :type required_snapshots: set[ZfsSnapshot]|None
    :rtype: ( ZfsSnapshot|ZfsBookmark|None, list[ZfsSnapshot], list[ZfsSnapshot], list[ZfsSnapshot], list[ZfsSnapshot] )

    Returns:
        tuple: A tuple containing:
            - ZfsSnapshot|ZfsBookmark|None: The source common snapshot
            - list[ZfsSnapshot]: Our obsolete source snapshots, after transfer is done. (will be thinned asap)
            - list[ZfsSnapshot]: Our obsolete target snapshots, after transfer is done. (will be thinned asap)
            - list[ZfsSnapshot]: Transfer target snapshots. These need to be transferred.
            - list[ZfsSnapshot]: Incompatible target snapshots. Target snapshots that are in the way, after the common snapshot. (need to be destroyed to continue)
    """

    ### 1: determine common and start snapshot if target already exists:
    if target_dataset.exists:
        target_dataset.debug("Determining start snapshot")
        (source_common_snapshot, target_common_snapshot) = source_dataset.find_common_snapshot(
            target_dataset, guid_check=guid_check, bookmark_tag=bookmark_tag)
        incompatible_target_snapshots = target_dataset.find_incompatible_snapshots(target_common_snapshot, raw)
    else:
        source_common_snapshot = None
        incompatible_target_snapshots = []

    # let thinner decide whats obsolete on source after the transfer is done
    if source_dataset.our_snapshots:
        source_obsoletes = source_dataset.thin_list()[1]
    else:
        source_obsoletes = []

    ### 2: determine possible target snapshots

    # start with snapshots that already exist, minus incompatibles
    if target_dataset.exists:
        possible_target_snapshots = [snapshot for snapshot in target_dataset.snapshots
                                     if snapshot not in incompatible_target_snapshots]
    else:
        possible_target_snapshots = []

    # add all snapshots from the source, starting after the common snapshot if it exists
    if source_common_snapshot is not None:
        source_snapshot = source_dataset.find_next_snapshot(source_common_snapshot)
    elif source_dataset.snapshots:
        source_snapshot = source_dataset.snapshots[0]
    else:
        source_snapshot = None

    while source_snapshot:
        # we want it?
        is_required = required_snapshots is not None and source_snapshot in required_snapshots
        if is_required and not source_snapshot.is_ours and not also_other_snapshots:
            source_snapshot.verbose("Including as clone origin for a selected clone")
        if (also_other_snapshots or source_snapshot.is_ours or is_required) \
                and not source_snapshot.is_snapshot_excluded:
            # create virtual target snapshot
            target_snapshot = target_dataset.zfs_node.get_snapshot(
                target_dataset.name + source_snapshot.typed_suffix, force_exists=False)
            possible_target_snapshots.append(target_snapshot)
        source_snapshot = source_dataset.find_next_snapshot(source_snapshot)

    ### 3: let the thinner decide what it wants by looking at all the possible target snapshots at once.
    # always keep the last target snapshot as common snapshot.
    if possible_target_snapshots:
        (target_keeps, target_obsoletes) = target_dataset.zfs_node.thin_list(
            possible_target_snapshots, keep_snapshots=[possible_target_snapshots[-1]])
    else:
        target_keeps = []
        target_obsoletes = []

    ### 4: look at what the thinner wants to keep and create a list of snapshots we still need to transfer
    target_transfers = [target_keep for target_keep in target_keeps if not target_keep.exists]

    return source_common_snapshot, source_obsoletes, target_obsoletes, target_transfers, incompatible_target_snapshots


def _active_properties(source_dataset, filter_properties, set_properties, property_format):
    """determine the property filter/set lists to use for this transfer

    :type source_dataset: ZfsContainer
    :type filter_properties: list[str]
    :type set_properties: list[str]
    :type property_format: str
    :rtype: (list[str], list[str])
    """

    (active_filter_properties, active_set_properties) = source_dataset.get_allowed_properties(filter_properties,
                                                                                              set_properties)

    # always filter properties that start with the property-format prefix (https://github.com/psy0rz/zfs_autobackup/issues/221)
    property_prefix = property_format.split(':')[0]
    for prop in source_dataset.properties:
        if prop.startswith(property_prefix):
            active_filter_properties.append(prop)

    return active_filter_properties, active_set_properties


def _post_transfer_source_cleanup(source_snapshot, prev_base, source_obsoletes, holds, use_bookmarks, bookmark_tag):
    """after source_snapshot was transferred: hold or bookmark it as the new common snapshot, and
    release/destroy the previous one if its obsolete. Returns the source-side point-in-time to use
    as base for the next incremental send.

    :type source_snapshot: ZfsSnapshot
    :type prev_base: ZfsPointInTime|None
    :type source_obsoletes: list[ZfsSnapshot]
    :type holds: bool
    :type use_bookmarks: bool
    :type bookmark_tag: str
    :rtype: ZfsPointInTime
    """

    if use_bookmarks:
        # bookmark the new common snapshot, so the snapshot itself can be destroyed when obsolete
        source_bookmark = source_snapshot.bookmark(bookmark_tag)
        if source_snapshot in source_obsoletes:
            source_snapshot.destroy()

        # TODO: make a better is_ours specially for bookmarks, as part of the next refactoring splitting in more classes
        # delete the previous bookmark if its ours (ends in our tag_separator + tag)
        if isinstance(prev_base, ZfsBookmark) and prev_base.name.endswith(
                source_snapshot.zfs_node.tag_seperator + bookmark_tag):
            prev_base.destroy()

        return source_bookmark

    # not using bookmarks: hold the new common snapshot instead
    if holds:
        source_snapshot.hold()

    # release hold and cleanup the previous common snapshot if its obsolete
    if isinstance(prev_base, ZfsSnapshot):
        prev_base.release()
        if prev_base in source_obsoletes:
            prev_base.destroy()

    return source_snapshot


def sync_snapshots(source_dataset, target_dataset, features, show_progress, filter_properties, set_properties,
                   ignore_recv_exit_code, holds, rollback, decrypt, encrypt, also_other_snapshots,
                   no_send, destroy_incompatible, send_pipes, recv_pipes, zfs_compressed, force, guid_check,
                   use_bookmarks, bookmark_tag, property_format, no_clone=True,
                   target_path=None, strip_path=0, required_snapshots=None):
    """sync source_dataset's snapshots to target_dataset, while also thinning out old snapshots along the way.

    :type source_dataset: ZfsContainer
    :type target_dataset: ZfsContainer
    :type features: list[str]
    :type show_progress: bool
    :type filter_properties: list[str]
    :type set_properties: list[str]
    :type ignore_recv_exit_code: bool
    :type holds: bool
    :type rollback: bool
    :type decrypt: bool
    :type encrypt: bool
    :type also_other_snapshots: bool
    :type no_send: bool
    :type destroy_incompatible: bool
    :type send_pipes: list[str]
    :type recv_pipes: list[str]
    :type zfs_compressed: bool
    :type force: bool
    :type guid_check: bool
    :type use_bookmarks: bool
    :type bookmark_tag: str
    :type property_format: str
    :type no_clone: bool
    :type target_path: str
    :type strip_path: int
    :type required_snapshots: set[ZfsSnapshot]|None
    """

    # determine how to handle encryption during the transfer:
    send_properties = True
    raw = False

    # source dataset encrypted?
    if source_dataset.properties.get('encryption', 'off') != 'off':
        # user wants to send it over decrypted?
        if decrypt:
            # when decrypting, zfs can't send properties
            send_properties = False
        else:
            # keep data encrypted by sending it raw (including properties)
            raw = True

    (source_common_snapshot, source_obsoletes, target_obsoletes, target_transfers,
     incompatible_target_snapshots) = \
        _plan_sync(source_dataset, target_dataset=target_dataset, also_other_snapshots=also_other_snapshots,
                   guid_check=guid_check, raw=raw, bookmark_tag=bookmark_tag,
                   required_snapshots=required_snapshots)

    if show_progress:
        source_dataset.zfs_node.logger.progress("Pre-cleaning..")

    # NOTE: we do a pre-clean because we dont want filesystems to fillup when backups keep failing.
    # Also usefull with no_send to still cleanup stuff.
    _pre_clean(
        source_dataset, target_dataset=target_dataset, source_common_snapshot=source_common_snapshot,
        source_obsoletes=source_obsoletes, target_obsoletes=target_obsoletes, target_transfers=target_transfers)

    # check if we can resume
    if len(target_transfers) > 0:
        if show_progress:
            source_dataset.zfs_node.logger.progress("Verifying resume token...")

        resume_token = _validate_resume_token(source_dataset, target_dataset, target_transfers[0])
    else:
        resume_token = None

    if show_progress:
        source_dataset.zfs_node.logger.progress("Preparing...")

    # handle incompatible stuff on target
    _handle_incompatible_target(target_dataset, incompatible_target_snapshots, destroy_incompatible, force,
                                source_common_snapshot, resume_token is not None)

    # now actually transfer the snapshots, if we want
    if no_send or len(target_transfers) == 0:
        return

    (active_filter_properties, active_set_properties) = _active_properties(source_dataset, filter_properties,
                                                                           set_properties, property_format)

    # encrypt at target?
    write_embedded = True
    if encrypt and not raw:
        # filter out encryption properties to let encryption on the target take place
        active_filter_properties.extend(["keylocation", "pbkdf2iters", "keyformat", "encryption"])
        write_embedded = False

    # When the target dataset is being created fresh and the source is a clone whose
    # origin's target equivalent exists, send the first snapshot as an incremental from
    # that origin so zfs recv reconstructs the clone relationship on the target.
    # Only resolve this now (not earlier) — checking target origin existence is only
    # valid when we know this is actually a new full transfer.
    if source_common_snapshot is None and not no_clone:
        clone_origin_snapshot = _resolve_clone_origin(source_dataset, target_dataset.zfs_node, target_path,
                                                      strip_path, guid_check)
    else:
        clone_origin_snapshot = None

    # the source-side point-in-time (snapshot or bookmark) to use as base for the next incremental send
    if clone_origin_snapshot is not None:
        incremental_base = clone_origin_snapshot
    else:
        incremental_base = source_common_snapshot

    prev_target_snapshot = target_dataset.find_snapshot(source_common_snapshot)

    do_rollback = rollback
    for target_snapshot in target_transfers:

        source_snapshot = source_dataset.find_snapshot(target_snapshot)
        assert source_snapshot is not None

        # do the rollback, one time at first transfer
        if do_rollback:
            if show_progress:
                source_dataset.zfs_node.logger.progress("Rolling back {}...".format(target_dataset))

            target_dataset.rollback()
            do_rollback = False

        source_snapshot.transfer_snapshot(target_snapshot, features=features,
                                          prev_snapshot=incremental_base, show_progress=show_progress,
                                          filter_properties=active_filter_properties,
                                          set_properties=active_set_properties,
                                          ignore_recv_exit_code=ignore_recv_exit_code,
                                          resume_token=resume_token, write_embedded=write_embedded, raw=raw,
                                          send_properties=send_properties, send_pipes=send_pipes,
                                          recv_pipes=recv_pipes, zfs_compressed=zfs_compressed, force=force)

        # the resume token is only valid for the first transfer
        resume_token = None

        # hold/release common snapshot on the target.
        if holds:
            target_snapshot.hold()

            if prev_target_snapshot:
                prev_target_snapshot.release()

        # cleanup on the source and determine the base for the next incremental send
        incremental_base = _post_transfer_source_cleanup(source_snapshot, incremental_base, source_obsoletes,
                                                         holds, use_bookmarks, bookmark_tag)

        # destroy the previous target snapshot if obsolete (usually this is only the common_snapshot,
        # the rest was already destroyed or will not be send)
        if prev_target_snapshot is not None and prev_target_snapshot in target_obsoletes:
            prev_target_snapshot.destroy()

        prev_target_snapshot = target_snapshot
