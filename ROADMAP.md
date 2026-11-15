# Roadmap

What is planned after 0.3.0. Each item links to its issue, where the details and the definition of done live. The issues are grouped in the [Now](https://github.com/pedrobtmartins/cx2iceberg/milestone/1) and [Next](https://github.com/pedrobtmartins/cx2iceberg/milestone/2) milestones and carry the [`roadmap`](https://github.com/pedrobtmartins/cx2iceberg/labels/roadmap) label. The current limitations are in the [README](README.md#limitations).

## Now

| Item | Why |
|---|---|
| [Run the tests in CI (#4)](https://github.com/pedrobtmartins/cx2iceberg/issues/4) | There is no CI yet. |
| [Publish releases (#5)](https://github.com/pedrobtmartins/cx2iceberg/issues/5) | 0.2.0 and 0.3.0 exist only in the changelog. |
| [End-to-end test on real AWS (#6)](https://github.com/pedrobtmartins/cx2iceberg/issues/6) | AWS is faked in the tests; the Athena path has never run against real services. |

## Next

**Writing to Iceberg**

| Item | Why |
|---|---|
| [Atomic `replace_by` (#7)](https://github.com/pedrobtmartins/cx2iceberg/issues/7) | A failure between the `DELETE` and the `INSERT` leaves the old rows deleted. |
| [Detect partition-spec changes (#8)](https://github.com/pedrobtmartins/cx2iceberg/issues/8) | The pre-check only compares column types. |
| [Delete rows `merge` no longer produces (#9)](https://github.com/pedrobtmartins/cx2iceberg/issues/9) | A child removed from the BOM stays in the table. |
| [Process large volumes in batches (#10)](https://github.com/pedrobtmartins/cx2iceberg/issues/10) | Everything is processed in memory today. |
| [Propose schema changes between model versions (#14)](https://github.com/pedrobtmartins/cx2iceberg/issues/14) | A new model version only makes the pre-check fail. |

**No-code: the Studio**

| Item | Why |
|---|---|
| [Build every transformation in the Studio (#12)](https://github.com/pedrobtmartins/cx2iceberg/issues/12) | Conditional values, composed text, partitioning and version mapping still need the YAML. |
| [Load from the Studio, no command line (#11)](https://github.com/pedrobtmartins/cx2iceberg/issues/11) | The Studio builds the configuration, but loading still needs the CLI. |
| [Ready-made configurations (#13)](https://github.com/pedrobtmartins/cx2iceberg/issues/13) | SerialPart, Batch, SingleLevelBomAsBuilt and PCF without configuration work. |

**Distribution**

| Item | Why |
|---|---|
| [Publish to PyPI (#15)](https://github.com/pedrobtmartins/cx2iceberg/issues/15) | Today it only installs from source. |

This roadmap is a plan, not a promise. Ideas and questions are welcome as [issues](https://github.com/pedrobtmartins/cx2iceberg/issues).
