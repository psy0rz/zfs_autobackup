from .ExecuteNode import ExecuteError
from .ThinnerRule import ThinnerRule
from .ZfsContainer import ZfsContainer
from .util import datetime_now


def thin_missing_targets(logger, target_dataset, used_target_datasets):
    """thin target datasets that are missing on the source.
    :type logger: LogConsole
    :type used_target_datasets: list[ZfsContainer]
    :type target_dataset: ZfsContainer
    """

    logger.debug("Thinning obsolete datasets")
    missing_datasets = [dataset for dataset in target_dataset.recursive_datasets if
                        dataset not in used_target_datasets]

    count = 0
    for dataset in missing_datasets:
        logger.debug("analyse missing {}".format(dataset))

        count = count + 1
        logger.progress("Analysing missing..", count, len(missing_datasets), 0)

        try:
            dataset.debug("Missing on source, thinning")
            dataset.thin()

        except Exception as e:
            dataset.error("Error during thinning of missing datasets ({})".format(str(e)))


def destroy_missing_targets(logger, target_dataset, used_target_datasets, destroy_missing, utc):
    """destroy target datasets that are missing on the source and that meet the requirements
    :type logger: LogConsole
    :type used_target_datasets: list[ZfsContainer]
    :type target_dataset: ZfsContainer
    :type destroy_missing: str
    :type utc: bool
    """

    logger.debug("Destroying obsolete datasets")

    missing_datasets = [dataset for dataset in target_dataset.recursive_datasets if
                        dataset not in used_target_datasets]

    count = 0
    for dataset in missing_datasets:

        count = count + 1
        logger.progress("Analysing destroy missing...", count, len(missing_datasets), 0)

        try:
            # cant do anything without our own snapshots
            if not dataset.our_snapshots:
                if dataset.datasets:
                    # its not a leaf, just ignore
                    dataset.debug("Destroy missing: ignoring")
                else:
                    dataset.verbose(
                        "Destroy missing: has no snapshots made by us (please destroy manually).")
            else:
                # past the deadline?
                deadline_ttl = ThinnerRule("0s" + destroy_missing).ttl
                now = datetime_now(utc).timestamp()
                if dataset.our_snapshots[-1].timestamp + deadline_ttl > now:
                    dataset.verbose("Destroy missing: Waiting for deadline.")
                else:

                    dataset.debug("Destroy missing: Removing our snapshots.")

                    # remove all our snaphots, except last, to safe space in case we fail later on
                    for snapshot in dataset.our_snapshots[:-1]:
                        snapshot.destroy(fail_exception=True)

                    # does it have other snapshots?
                    has_others = False
                    for snapshot in dataset.snapshots:
                        if not snapshot.is_ours:
                            has_others = True
                            break

                    if has_others:
                        dataset.verbose("Destroy missing: Still in use by other snapshots")
                    else:
                        if dataset.datasets:
                            dataset.verbose("Destroy missing: Still has children here.")
                        else:
                            dataset.verbose("Destroy missing.")
                            dataset.our_snapshots[-1].destroy(fail_exception=True)
                            dataset.destroy(fail_exception=True)

        except Exception as e:
            dataset.error("Error during --destroy-missing: {}".format(str(e)))


def _resolve_clone_origin(make_target_name, guid_check, source_dataset, target_node):
    """Return the source-side origin snapshot to use as zfs send -i base for a clone,
    or None if the clone relationship cannot be preserved on the target.

    :type make_target_name: callable
    :type guid_check: bool
    :type source_dataset: ZfsContainer
    :type target_node: ZfsNode
    :rtype: ZfsSnapshot|None
    """

    source_origin_snap = source_dataset.get_clone_origin_snapshot()
    if source_origin_snap is None:
        return None

    try:
        target_origin_parent = make_target_name(source_origin_snap.parent)
    except Exception as e:
        source_dataset.warning("Cannot replicate as clone: cannot map origin '{}' to target ({}). Falling back to full send.".format(source_origin_snap.name, str(e)))
        return None

    target_origin_snap = target_node.get_dataset(target_origin_parent + '@' + source_origin_snap.suffix)

    if not target_origin_snap.exists:
        source_dataset.warning("Cannot replicate as clone: origin '{}' not available on target. Falling back to full send.".format(source_origin_snap.name))
        return None

    if guid_check:
        try:
            if source_origin_snap.properties.get('guid') != target_origin_snap.properties.get('guid'):
                source_dataset.warning("Cannot replicate as clone: origin guid mismatch between source and target. Falling back to full send.")
                return None
        except ExecuteError:
            # properties not readable (e.g. test mode after a simulated recv) — skip the check
            source_dataset.debug("Cannot read origin guid; skipping guid check for clone replication.")

    return source_origin_snap


def _topological_sort_for_clones(logger, source_datasets):
    """Reorder source_datasets so an origin is processed before any selected clone of it.
    Datasets whose origin is not in the selection (or who are not clones) are independent
    and keep their relative order. On a detected cycle (should not happen with valid ZFS)
    the original list is returned with a warning.

    :type logger: LogConsole
    :type source_datasets: list[ZfsContainer]
    :rtype: list[ZfsContainer]
    """

    by_name = {d.name: d for d in source_datasets}

    parent_dep = {}  # dataset_name -> parent_dataset_name in selection, or None
    for d in source_datasets:
        try:
            origin = d.properties.get('origin', '-')
        except Exception:
            origin = '-'
        if origin == '-' or '@' not in origin:
            parent_dep[d.name] = None
            continue
        parent_path = origin.split('@', 1)[0]
        if parent_path in by_name and parent_path != d.name:
            parent_dep[d.name] = parent_path
        else:
            parent_dep[d.name] = None

    indegree = {name: 0 for name in by_name}
    children = {name: [] for name in by_name}
    for name, parent in parent_dep.items():
        if parent is not None:
            indegree[name] += 1
            children[parent].append(name)

    queue = [d.name for d in source_datasets if indegree[d.name] == 0]
    result = []
    while queue:
        name = queue.pop(0)
        result.append(by_name[name])
        for child in children[name]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(result) != len(source_datasets):
        logger.warning("Clone topological sort: cycle detected, keeping original dataset order.")
        return source_datasets

    return result


def _required_origin_snapshots(source_datasets):
    """Build a map parent_dataset_name -> set of origin snapshot full names that
    downstream clones (also in the selection) need pinned on the target. Used to
    force-include those specific snapshots even without --other-snapshots.

    :type source_datasets: list[ZfsContainer]
    :rtype: dict[str, set[str]]
    """

    by_name = {d.name: d for d in source_datasets}
    required = {}
    for d in source_datasets:
        try:
            origin = d.properties.get('origin', '-')
        except Exception:
            origin = '-'
        if origin == '-' or '@' not in origin:
            continue
        parent_path = origin.split('@', 1)[0]
        if parent_path in by_name and parent_path != d.name:
            required.setdefault(parent_path, set()).add(origin)
    return required


def sync_datasets(logger, source_node, source_datasets, target_node, bookmark_tag,
                  send_pipes, recv_pipes, make_target_name,
                  no_clone, no_send, no_bookmarks, no_thinning,
                  filter_properties, set_properties,
                  ignore_transfer_errors, holds, rollback, other_snapshots,
                  destroy_incompatible, decrypt, encrypt, zfs_compressed, force,
                  guid_check, property_format, debug,
                  target_path, destroy_missing, utc):
    """Sync datasets, or thin-only on both sides.
    :type logger: LogConsole
    :type source_node: ZfsNode
    :type source_datasets: list[ZfsContainer]
    :type target_node: ZfsNode
    :type bookmark_tag: str
    :type send_pipes: list
    :type recv_pipes: list
    :type make_target_name: callable
    :type no_clone: bool
    :type no_send: bool
    :type no_bookmarks: bool
    :type no_thinning: bool
    :type filter_properties: list[str]
    :type set_properties: list[str]
    :type ignore_transfer_errors: bool
    :type holds: bool
    :type rollback: bool
    :type other_snapshots: bool
    :type destroy_incompatible: bool
    :type decrypt: bool
    :type encrypt: bool
    :type zfs_compressed: bool
    :type force: bool
    :type guid_check: bool
    :type property_format: str
    :type debug: bool
    :type target_path: str
    :type destroy_missing: str|None
    :type utc: bool
    :rtype: int
    """

    if not no_clone:
        source_datasets = _topological_sort_for_clones(logger, source_datasets)
        required_origin_snapshots = _required_origin_snapshots(source_datasets)
    else:
        required_origin_snapshots = {}

    fail_count = 0
    count = 0
    target_datasets = []
    for source_dataset in source_datasets:

        count = count + 1
        logger.progress("Analysing dataset...", count, len(source_datasets), fail_count)

        try:
            # determine corresponding target_dataset
            target_name = make_target_name(source_dataset)
            target_dataset = target_node.get_dataset(target_name)
            assert isinstance(target_dataset, ZfsContainer)
            target_datasets.append(target_dataset)

            # ensure parents exists
            # TODO: this isnt perfect yet, in some cases it can create parents when it shouldn't.
            target_parent = target_dataset.parent
            if not no_send \
                    and target_parent is not None \
                    and target_parent not in target_datasets \
                    and not target_parent.exists:
                target_dataset.debug("Creating unmountable parents")
                target_parent.create_filesystem(parents=True)

            # determine common zpool features (cached, so no problem we call it often)
            source_features = source_node.get_pool(source_dataset).features
            target_features = target_node.get_pool(target_dataset).features
            common_features = [f for f in source_features if f in target_features]

            if no_bookmarks:
                use_bookmarks = False
            else:
                # NOTE: bookmark_written seems to be needed. (only 'bookmarks' was not enough on ubuntu 20)
                if 'bookmark_written' not in common_features:
                    source_dataset.warning("Disabling bookmarks, not supported on both pools.")
                    use_bookmarks = False
                else:
                    use_bookmarks = True

            # if the source is a clone and the target-side origin is available, send
            # the first snapshot as an incremental from the origin so the clone
            # relationship is preserved on the target.
            clone_origin_snapshot = None
            if not no_clone:
                clone_origin_snapshot = _resolve_clone_origin(make_target_name, guid_check, source_dataset, target_node)

            # sync the snapshots of this dataset
            source_dataset.sync_snapshots(target_dataset, show_progress=True,
                                          features=common_features, filter_properties=filter_properties,
                                          set_properties=set_properties,
                                          ignore_recv_exit_code=ignore_transfer_errors,
                                          holds=holds, rollback=rollback,
                                          also_other_snapshots=other_snapshots,
                                          no_send=no_send,
                                          destroy_incompatible=destroy_incompatible,
                                          send_pipes=send_pipes, recv_pipes=recv_pipes,
                                          decrypt=decrypt, encrypt=encrypt,
                                          zfs_compressed=zfs_compressed, force=force,
                                          guid_check=guid_check, use_bookmarks=use_bookmarks,
                                          bookmark_tag=bookmark_tag,
                                          property_format=property_format,
                                          clone_origin_snapshot=clone_origin_snapshot,
                                          required_snapshots=required_origin_snapshots.get(source_dataset.name))
        except Exception as e:

            fail_count = fail_count + 1
            source_dataset.error("FAILED: " + str(e))
            if debug:
                logger.verbose("Debug mode, aborting on first error")
                raise

    target_path_dataset = target_node.get_dataset(target_path)
    assert isinstance(target_path_dataset, ZfsContainer)
    if not no_thinning:
        thin_missing_targets(logger, target_dataset=target_path_dataset, used_target_datasets=target_datasets)

    if destroy_missing is not None:
        destroy_missing_targets(logger, target_dataset=target_path_dataset, used_target_datasets=target_datasets,
                                destroy_missing=destroy_missing, utc=utc)

    return fail_count
