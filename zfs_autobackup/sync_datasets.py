"""Dataset-level replication: sync a selection of source datasets to a target node,
and thin or destroy target datasets that are missing on the source."""

from collections import deque

from .ThinnerRule import ThinnerRule
from .ZfsContainer import ZfsContainer
from .sync_snapshots import sync_snapshots
from .util import datetime_now


def _missing_datasets(target_dataset, used_target_datasets):
    """list the datasets under target_dataset that are not used as a backup target (anymore)

    :type target_dataset: ZfsContainer
    :type used_target_datasets: list[ZfsContainer]
    :rtype: list[ZfsContainer]
    """

    return [dataset for dataset in target_dataset.recursive_datasets if dataset not in used_target_datasets]


def thin_missing_targets(logger, target_dataset, used_target_datasets):
    """thin target datasets that are missing on the source.

    :type logger: LogConsole
    :type target_dataset: ZfsContainer
    :type used_target_datasets: list[ZfsContainer]
    """

    logger.debug("Thinning obsolete datasets")
    missing_datasets = _missing_datasets(target_dataset, used_target_datasets)

    for count, dataset in enumerate(missing_datasets, start=1):
        logger.debug("analyse missing {}".format(dataset))
        logger.progress("Analysing missing..", count, len(missing_datasets), 0)

        try:
            dataset.debug("Missing on source, thinning")
            dataset.thin()
        except Exception as e:
            dataset.error("Error during thinning of missing datasets ({})".format(str(e)))


def _destroy_missing_dataset(dataset, destroy_missing, utc):
    """destroy a single missing dataset if it meets all the requirements, otherwise tell the user why not.

    :type dataset: ZfsContainer
    :type destroy_missing: str
    :type utc: bool
    """

    # cant do anything without our own snapshots
    if not dataset.our_snapshots:
        if dataset.datasets:
            # its not a leaf, just ignore
            dataset.debug("Destroy missing: ignoring")
        else:
            dataset.verbose("Destroy missing: has no snapshots made by us (please destroy manually).")
        return

    # past the deadline?
    deadline_ttl = ThinnerRule("0s" + destroy_missing).ttl
    now = datetime_now(utc).timestamp()
    if dataset.our_snapshots[-1].timestamp + deadline_ttl > now:
        dataset.verbose("Destroy missing: Waiting for deadline.")
        return

    dataset.debug("Destroy missing: Removing our snapshots.")

    # remove all our snaphots, except last, to safe space in case we fail later on
    for snapshot in dataset.our_snapshots[:-1]:
        snapshot.destroy(fail_exception=True)

    # does it have other snapshots?
    if any(not snapshot.is_ours for snapshot in dataset.snapshots):
        dataset.verbose("Destroy missing: Still in use by other snapshots")
        return

    if dataset.datasets:
        dataset.verbose("Destroy missing: Still has children here.")
        return

    dataset.verbose("Destroy missing.")
    dataset.our_snapshots[-1].destroy(fail_exception=True)
    dataset.destroy(fail_exception=True)


def destroy_missing_targets(logger, target_dataset, used_target_datasets, destroy_missing, utc):
    """destroy target datasets that are missing on the source and that meet the requirements

    :type logger: LogConsole
    :type target_dataset: ZfsContainer
    :type used_target_datasets: list[ZfsContainer]
    :type destroy_missing: str
    :type utc: bool
    """

    logger.debug("Destroying obsolete datasets")
    missing_datasets = _missing_datasets(target_dataset, used_target_datasets)

    for count, dataset in enumerate(missing_datasets, start=1):
        logger.progress("Analysing destroy missing...", count, len(missing_datasets), 0)

        try:
            _destroy_missing_dataset(dataset, destroy_missing, utc)
        except Exception as e:
            dataset.error("Error during --destroy-missing: {}".format(str(e)))


def _clone_dependencies(source_datasets):
    """yield (clone_dataset, origin_snapshot, origin_dataset) for every selected dataset that is a
    clone of another selected dataset.

    :type source_datasets: list[ZfsContainer]
    """

    for dataset in source_datasets:
        origin_snapshot = dataset.origin
        if origin_snapshot is None:
            continue
        origin_dataset = origin_snapshot.parent
        # only a dependency if the origin dataset is itself in the selection
        if origin_dataset in source_datasets and origin_dataset is not dataset:
            yield dataset, origin_snapshot, origin_dataset


def _topological_sort_for_clones(logger, source_datasets):
    """Reorder source_datasets so an origin is processed before any selected clone of it.
    Datasets whose origin is not in the selection (or who are not clones) are independent
    and keep their relative order. On a detected cycle (should not happen with valid ZFS)
    the original list is returned with a warning.

    :type logger: LogConsole
    :type source_datasets: list[ZfsContainer]
    :rtype: list[ZfsContainer]
    """

    # build edge map: for each dataset that is a clone of another selected dataset,
    # record that dependency so the origin is emitted first.
    indegree = {dataset: 0 for dataset in source_datasets}
    children = {dataset: [] for dataset in source_datasets}
    for clone_dataset, origin_snapshot, origin_dataset in _clone_dependencies(source_datasets):
        indegree[clone_dataset] += 1
        children[origin_dataset].append(clone_dataset)

    # Kahn's algorithm: start with datasets that have no in-selection origin
    queue = deque(dataset for dataset in source_datasets if indegree[dataset] == 0)
    result = []
    while queue:
        dataset = queue.popleft()
        result.append(dataset)
        for child in children[dataset]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    # a cycle means ZFS state is inconsistent; fall back to original order
    if len(result) != len(source_datasets):
        logger.warning("Clone topological sort: cycle detected, keeping original dataset order.")
        return source_datasets

    return result


def _required_origin_snapshots(source_datasets):
    """Build a map origin_dataset -> set of origin snapshots that downstream clones (also in the
    selection) need pinned on the target. Used to force-include those specific snapshots even
    without --other-snapshots.

    :type source_datasets: list[ZfsContainer]
    :rtype: dict[ZfsContainer, set[ZfsSnapshot]]
    """

    required = {}
    for clone_dataset, origin_snapshot, origin_dataset in _clone_dependencies(source_datasets):
        required.setdefault(origin_dataset, set()).add(origin_snapshot)
    return required


def sync_datasets(logger, source_node, source_datasets, target_node, bookmark_tag,
                  send_pipes, recv_pipes, target_path, strip_path,
                  no_clone, no_send, no_bookmarks, no_thinning,
                  filter_properties, set_properties,
                  ignore_transfer_errors, holds, rollback, other_snapshots,
                  destroy_incompatible, decrypt, encrypt, zfs_compressed, force,
                  guid_check, property_format, debug,
                  target_dataset_base, destroy_missing, utc):
    """Sync datasets, or thin-only on both sides.

    :type logger: LogConsole
    :type source_node: ZfsNode
    :type source_datasets: list[ZfsContainer]
    :type target_node: ZfsNode
    :type bookmark_tag: str
    :type send_pipes: list[str]
    :type recv_pipes: list[str]
    :type target_path: str
    :type strip_path: int
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
    :type target_dataset_base: ZfsContainer
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
    target_datasets = []
    bookmarks_unsupported_pools = set()  # source pools we've already warned about
    for count, source_dataset in enumerate(source_datasets, start=1):

        logger.progress("Analysing dataset...", count, len(source_datasets), fail_count)

        try:
            # determine corresponding target_dataset
            target_name = source_dataset.map_to_target_path(target_path, strip_path)
            target_dataset = target_node.get_container(target_name)
            target_datasets.append(target_dataset)

            # ensure parents exists
            target_parent = target_dataset.parent
            if not no_send \
                    and target_parent is not None \
                    and target_parent not in target_datasets \
                    and not target_parent.exists:
                target_dataset.debug("Creating unmountable parents")
                target_parent.create_filesystem(parents=True)

            # determine common zpool features
            source_pool = source_node.get_pool(source_dataset)
            source_features = source_pool.features
            target_features = target_node.get_pool(target_dataset).features
            common_features = [feature for feature in source_features if feature in target_features]

            use_bookmarks = not no_bookmarks
            if use_bookmarks and 'bookmarks' not in common_features:
                if source_pool.name not in bookmarks_unsupported_pools:
                    source_pool.warning("Disabling bookmarks, not supported on both pools.")
                    bookmarks_unsupported_pools.add(source_pool.name)
                use_bookmarks = False

            # sync the snapshots of this dataset
            sync_snapshots(source_dataset, target_dataset, show_progress=True,
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
                           no_clone=no_clone,
                           target_path=target_path, strip_path=strip_path,
                           required_snapshots=required_origin_snapshots.get(source_dataset))
        except Exception as e:
            fail_count = fail_count + 1
            source_dataset.error("FAILED: " + str(e))
            if debug:
                logger.verbose("Debug mode, aborting on first error")
                raise

    if not no_thinning:
        thin_missing_targets(logger, target_dataset=target_dataset_base, used_target_datasets=target_datasets)

    if destroy_missing is not None:
        destroy_missing_targets(logger, target_dataset=target_dataset_base, used_target_datasets=target_datasets,
                                destroy_missing=destroy_missing, utc=utc)

    return fail_count
