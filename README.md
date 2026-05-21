# Needle

<!-- --8<-- [start:intro] -->

This project originated as part of the [ADACS Merit Allocation Program](https://adacs.org.au/merit-allocation-program/).

Needle is an ETL pipeline for finding compact sources in a radio observation. It uses [Prefect](https://www.prefect.io/) for workflow orchestration, logging and event triggering.

Needle containerises its heavy requirements - CASA and WSClean - so that the user doesn't need to install them to the working system. **Public images to be provided**.

Needle also exposes its Python modules via CLI entrypoints to allow to user to explore their function independently of the main workflow.

It can work with `.uvfits`, `.mir` or `.ms` formatted observations.

<!-- --8<-- [end:intro] -->

For more details, see the [documentation](https://rchhetri-2026a.readthedocs.io/en/latest/).
