from .ZfsBookmark import ZfsBookmark
from .ZfsContainer import ZfsContainer
from .ZfsPointInTime import ZfsPointInTime
from .ZfsSnapshot import ZfsSnapshot


def _pre_clean(source_dataset, source_common_snapshot, target_dataset, source_obsoletes, target_obsoletes, target_transfers):
    """cleanup old stuff on the source before starting snapshot syncing

    :type source_dataset: ZfsContainer
    :type source_common_snapshot: ZfsSnapshot|ZfsBookmark|None
    :type target_dataset: ZfsContainer
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

    # on target: destroy everything thats obsolete, except common_snapshot
    if target_dataset.exists:
        for target_snapshot in target_dataset.snapshots:
            if (target_snapshot in target_obsoletes) \
                    and (not source_common_snapshot or (
                    target_snapshot.tagless_suffix != source_common_snapshot.tagless_suffix)):
                if target_snapshot.exists:
                    target_snapshot.destroy()


def handle_incompatible_target(target_dataset, incompatible_target_snapshots, destroy_incompatible, force, source_common_snapshot, is_resume):
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

    #from this point on things get progressively worse..

    if source_common_snapshot:
        # we just need to delete some incompatibles:
        for snapshot in incompatible_target_snapshots:
            snapshot.warning("Incompatible snapshot")

        if not destroy_incompatible:
            raise (Exception("Use --destroy-incompatible to get rid of these."))

    else:
        # no common snapshot, so the whole dataset is incompatible
        if incompatible_target_snapshots:
            if not force or not destroy_incompatible:
                target_dataset.error("Incompatible dataset!")
                raise (Exception("Use --destroy-incompatible -F to overwrite the target dataset and start over."))
        else:
            if not force:
                target_dataset.error("Incompatible dataset!")
                raise (Exception("Use -F to overwrite the target dataset and start over."))

        target_dataset.warning("Overwriting incompatible dataset.")

    # remove incompatibles and rollback
    for snapshot in incompatible_target_snapshots:
        snapshot.destroy(fail_exception=True)

    target_dataset.invalidate_cache()
    target_dataset.rollback()


def _validate_resume_token(source_dataset, target_dataset, start_snapshot):
    """validate and get (or destroy) resume token

    :type source_dataset: ZfsContainer
    :type target_dataset: ZfsContainer
    :type start_snapshot: ZfsSnapshot|None
    :rtype: str|None
    """

    if target_dataset.exists and 'receive_resume_token' in target_dataset.properties:
        if start_snapshot is None:
            target_dataset.verbose("Aborting resume, its obsolete.")
            target_dataset.abort_resume()
        else:
            resume_token = target_dataset.properties['receive_resume_token']
            # not valid anymore?
            resume_snapshot = source_dataset.zfs_node.get_resume_snapshot(resume_token)
            if not resume_snapshot or start_snapshot.suffix != resume_snapshot.suffix:
                target_dataset.verbose("Aborting resume, its no longer valid.")
                target_dataset.abort_resume()
            else:
                return resume_token

    return None


def _plan_sync(source_dataset, target_dataset, also_other_snapshots, guid_check, raw, bookmark_tag, required_snapshots=None):
    """Determine at what snapshot to start syncing to target_dataset and what to sync and what to keep.

    :type source_dataset: ZfsContainer
    :type target_dataset: ZfsContainer
    :type also_other_snapshots: bool
    :type guid_check: bool
    :type raw: bool
    :type bookmark_tag: str
    :type required_snapshots: set[ZfsSnapshot]|None
    :rtype: ( ZfsSnapshot|ZfsBookmark, list[ZfsSnapshot], list[ZfsSnapshot], list[ZfsSnapshot], list[ZfsSnapshot] )

    Returns:
        tuple: A tuple containing:
            - ZfsSnapshot|ZfsBookmark: The source common snapshot
            - list[ZfsSnapshot]: Our obsolete source snapshots, after transfer is done. (will be thinned asap)
            - list[ZfsSnapshot]: Our obsolete target snapshots, after transfer is done. (will be thinned asap)
            - list[ZfsSnapshot]: Transfer target snapshots. These need to be transferred.
            - list[ZfsSnapshot]: Incompatible target snapshots. Target snapshots that are in the way, after the common snapshot. (need to be destroyed to continue)
    """

    ### 1: determine common and start snapshot if target already exists:

    if target_dataset.exists:
        target_dataset.debug("Determining start snapshot")
        (source_common_snapshot, target_common_snapshot) = source_dataset.find_common_snapshot(target_dataset,
                                                                                               guid_check=guid_check,
                                                                                               bookmark_tag=bookmark_tag)
        incompatible_target_snapshots = target_dataset.find_incompatible_snapshots(target_common_snapshot, raw)
    else:
        source_common_snapshot = None
        incompatible_target_snapshots = []

    # let thinner decide whats obsolete on source after the transfer is done
    source_obsoletes = []
    if source_dataset.our_snapshots:
        source_obsoletes = source_dataset.thin_list()[1]

    ### 2: Determine possible target snapshots

    # start with snapshots that already exist, minus incompatibles
    if target_dataset.exists:
        possible_target_snapshots = [snapshot for snapshot in target_dataset.snapshots if
                                     snapshot not in incompatible_target_snapshots]
    else:
        possible_target_snapshots = []

    # add all snapshots from the source, starting after the common snapshot if it exists
    if source_common_snapshot is not None:
        source_snapshot = source_dataset.find_next_snapshot(source_common_snapshot)
    else:
        if source_dataset.snapshots:
            source_snapshot = source_dataset.snapshots[0]
        else:
            source_snapshot = None

    while source_snapshot:
        # we want it?
        is_required = required_snapshots is not None and source_snapshot in required_snapshots
        if is_required and not source_snapshot.is_ours and not also_other_snapshots:
            source_snapshot.verbose("Including as clone origin for a selected clone")
        if (also_other_snapshots or source_snapshot.is_ours or is_required) and not source_snapshot.is_snapshot_excluded:
            # create virtual target snapshot
            target_snapshot = target_dataset.zfs_node.get_snapshot(
                target_dataset.name + source_snapshot.typed_suffix, force_exists=False)
            possible_target_snapshots.append(target_snapshot)
        source_snapshot = source_dataset.find_next_snapshot(source_snapshot)

    ### 3: Let the thinner decide what it wants by looking at all the possible target_snaphots at once.
    # always keep the last target snapshot as common snapshot.
    if possible_target_snapshots:
        (target_keeps, target_obsoletes) = target_dataset.zfs_node.thin_list(possible_target_snapshots,
                                                                             keep_snapshots=[
                                                                                 possible_target_snapshots[-1]])
    else:
        target_keeps = []
        target_obsoletes = []

    ### 4: Look at what the thinner wants and create a list of snapshots we still need to transfer
    target_transfers = []
    for target_keep in target_keeps:
        if not target_keep.exists:
            target_transfers.append(target_keep)

    return source_common_snapshot, source_obsoletes, target_obsoletes, target_transfers, incompatible_target_snapshots


def sync_snapshots(source_dataset, target_dataset, features, show_progress, filter_properties, set_properties,
                   ignore_recv_exit_code, holds, rollback, decrypt, encrypt, also_other_snapshots,
                   no_send, destroy_incompatible, send_pipes, recv_pipes, zfs_compressed, force, guid_check,
                   use_bookmarks, bookmark_tag, property_format, clone_origin_snapshot=None,
                   required_snapshots=None):
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
    :type clone_origin_snapshot: ZfsSnapshot|None
    :type required_snapshots: set[ZfsSnapshot]|None
    """

    # defaults for these settings if there is no encryption stuff going on:
    send_properties = True
    raw = False
    write_embedded = True

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
        source_dataset,
        source_common_snapshot=source_common_snapshot, target_dataset=target_dataset,
        target_transfers=target_transfers, target_obsoletes=target_obsoletes, source_obsoletes=source_obsoletes)

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
    handle_incompatible_target(target_dataset, incompatible_target_snapshots, destroy_incompatible, force, source_common_snapshot, resume_token is not None)

    # now actually transfer the snapshots, if we want
    if no_send or len(target_transfers) == 0:
        return

    (active_filter_properties, active_set_properties) = source_dataset.get_allowed_properties(filter_properties,
                                                                                               set_properties)

    # always filter properties that start with the property-format prefix (https://github.com/psy0rz/zfs_autobackup/issues/221)
    property_prefix = property_format.split(':')[0]
    for prop in source_dataset.properties:
        if prop.startswith(property_prefix):
            active_filter_properties.append(prop)

    # encrypt at target?
    if encrypt and not raw:
        # filter out encryption properties to let encryption on the target take place
        active_filter_properties.extend(["keylocation", "pbkdf2iters", "keyformat", "encryption"])
        write_embedded = False

    # now actually transfer the snapshots
    do_rollback = rollback
    # When the target dataset is being created fresh and the source is a clone whose
    # origin's target equivalent exists, send the first snapshot as an incremental from
    # that origin so zfs recv reconstructs the clone relationship on the target.
    if source_common_snapshot is None and clone_origin_snapshot is not None:
        prev_source_snapshot_bookmark = clone_origin_snapshot
    else:
        prev_source_snapshot_bookmark = source_common_snapshot
    prev_target_snapshot = target_dataset.find_snapshot(source_common_snapshot)
    for target_snapshot in target_transfers:

        source_snapshot = source_dataset.find_snapshot(target_snapshot)
        assert source_snapshot is not None

        # do the rollback, one time at first transfer
        if do_rollback:
            if show_progress:
                source_dataset.zfs_node.logger.progress(f"Rolling back {target_dataset}...")

            target_dataset.rollback()
            do_rollback = False

        source_snapshot.transfer_snapshot(target_snapshot, features=features,
                                          prev_snapshot=prev_source_snapshot_bookmark, show_progress=show_progress,
                                          filter_properties=active_filter_properties,
                                          set_properties=active_set_properties,
                                          ignore_recv_exit_code=ignore_recv_exit_code,
                                          resume_token=resume_token, write_embedded=write_embedded, raw=raw,
                                          send_properties=send_properties, send_pipes=send_pipes,
                                          recv_pipes=recv_pipes, zfs_compressed=zfs_compressed, force=force)

        resume_token = None

        # hold/release common snapshot on the target.
        if holds:
            target_snapshot.hold()

            if prev_target_snapshot:
                prev_target_snapshot.release()

        if use_bookmarks:
            # bookmark common snapshot, and clean up obsolete snapshots and bookmark
            source_bookmark = source_snapshot.bookmark(bookmark_tag)
            if source_snapshot in source_obsoletes:
                source_snapshot.destroy()

            # TODO: make a better is_ours specially for bookmarks, as part of the next refactoring splitting in more classes
            # delete any bookmark that ends in ours tag_separator + tag.
            if prev_source_snapshot_bookmark and type(
                    prev_source_snapshot_bookmark) is ZfsBookmark and prev_source_snapshot_bookmark.name.endswith(
                    source_dataset.zfs_node.tag_seperator + bookmark_tag):
                prev_source_snapshot_bookmark.destroy()

        # dont use bookmarks
        else:
            source_bookmark = None
            if holds:
                source_snapshot.hold()

            # release hold, cleanup obsolete snapshot
            if isinstance(prev_source_snapshot_bookmark, ZfsSnapshot):
                prev_source_snapshot_bookmark.release()
                if prev_source_snapshot_bookmark in source_obsoletes:
                    prev_source_snapshot_bookmark.destroy()

        # destroy the previous target snapshot if obsolete (usually this is only the common_snapshot,
        # the rest was already destroyed or will not be send)
        if prev_target_snapshot is not None and prev_target_snapshot in target_obsoletes:
            prev_target_snapshot.destroy()

        # we always try to use the bookmark during incremental send
        if source_bookmark:
            prev_source_snapshot_bookmark = source_bookmark
        else:
            prev_source_snapshot_bookmark = source_snapshot

        prev_target_snapshot = target_snapshot
