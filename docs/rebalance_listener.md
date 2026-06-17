# Rebalance Listener

When partitions are `assigned` or `revoked` to a stream, a `rebalance` is triggered. Different accions can be performed, depending on your use cases, for example:

- Cleanup or custom state save on the start of a rebalance operation
- Saving offsets in a custom store when a partition is `revoked`
- Load a state or cache warmup on completion of a successful partition re-assignment.

To do that, you will need a `RebalanceListener`.

### Metrics Rebalance Listener

Kstreams use a default listener for all the streams to clean the metrics after a rebalance takes place

::: kstreams.MetricsRebalanceListener
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
        show_bases: false
        show_source: false

### Manual Commit

If `manual` commit is enabled, you migh want to use the `ManualCommitRebalanceListener`. This `rebalance listener` will call `commit`
before the `stream` partitions are revoked to avoid the error `CommitFailedError` and *duplicate* message delivery after a rebalance. See code [example](https://github.com/kpn/kstreams/tree/master/examples/stream-with-manual-commit) with
manual `commit`

::: kstreams.ManualCommitRebalanceListener
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
        show_bases: false
        show_source: false

!!! note
    `ManualCommitRebalanceListener` also includes the `MetricsRebalanceListener` funcionality.

### Custom Rebalance Listener

If you want to define a custom `RebalanceListener`, it has to inherits from `kstreams.RebalanceListener`.

::: kstreams.RebalanceListener
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false
        show_bases: false
        show_source: false

!!! note
    It also possible to inherits from `ManualCommitRebalanceListener` and `MetricsRebalanceListener`
