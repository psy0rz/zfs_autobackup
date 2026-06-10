from .ThinnerRule import ThinnerRule
from .ZfsContainer import ZfsContainer
from .sync_snapshots import sync_snapshots
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
    indegree = {d: 0 for d in source_datasets}
    children = {d: [] for d in source_datasets}
    for d in source_datasets:
        origin = d.origin
        if origin is None:
            continue
        parent = origin.parent
        # only a dependency if the origin dataset is itself in the selection
        if parent in source_datasets and parent is not d:
            indegree[d] += 1
            children[parent].append(d)

    # Kahn's algorithm: start with datasets that have no in-selection origin
    queue = [d for d in source_datasets if indegree[d] == 0]
    result = []
    while queue:
        d = queue.pop(0)
        result.append(d)
        for child in children[d]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    # a cycle means ZFS state is inconsistent; fall back to original order
    if len(result) != len(source_datasets):
        logger.warning("Clone topological sort: cycle detected, keeping original dataset order.")
        return source_datasets

    return result


def _required_origin_snapshots(source_datasets):
    """Build a map parent_dataset_name -> set of origin snapshot full names that
    downstream clones (also in the selection) need pinned on the target. Used to
    force-include those specific snapshots even without --other-snapshots.

    :type source_datasets: list[ZfsContainer]
    :rtype: dict[ZfsContainer, set[ZfsSnapshot]]
    """

    required = {}
    for d in source_datasets:
        origin = d.origin
        if origin is None:
            continue
        parent = origin.parent
        if parent in source_datasets and parent is not d:
            required.setdefault(parent, set()).add(origin)
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
    :type send_pipes: list
    :type recv_pipes: list
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
    :type target_dataset: ZfsContainer
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
            target_name = source_dataset.map_to_target_path(target_path, strip_path)
            target_dataset = target_node.get_container(target_name)
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
