# Pipeline Reference

The Needle pipeline is a sequence of processing stages. Each stage can be configured by passing the appropriate configuration object to the pipeline via [NeedleConfig][needle.config.pipeline.NeedleConfig].

The pipeline is parallelised using Prefect and a Dask task runner.
This doc serves as a reference detailing these stages and how they map to configuration.

### Beam Discovery & Setup

Scans the provided working directory for beam pairs. A beam pair is a calibration and target observation grouped by their beam number. Each beam pair is then set up with its own working directory in preparation for processing.

**Configuration:** None

**Side Effects:** Observation files grouped and placed in their respective beam directories

### Conversion

Beam pairs are converted to measurement set (.ms) format where they are not already in .ms format.

**Configuration:** None

**Side Effects:** Measurement sets created if input was not already in .ms format

### Flagging

Both the calibator and target measurement sets are flagged for bad data/antennas. Uses CASA for flag operations.

**Configuration:** [FlagConfig][needle.config.flag.FlagConfig]

**Side Effects:** Calibrator and Target .ms files updated in-place

### Calibration

Using the calibrator source, determines calibration solutions and applies them to the target source in a new, _calibrated target_ measurement set. Uses CASA for calibration operations.

**Configuration:** [CalibrateConfig][needle.config.calibrate.CalibrateConfig]

**Side Effects:** .gcal and .bpcal solutions created. Calibrated target .ms created.

### Inspection & Diagnostics

Runs an inspection on the beam pair and outputs the results to a JSON format file. The inspection documents the basic information of the MS such as the number of intervals (used later), obersvation time, number of antennas etc.

Runs a diagnostics suite on the calibrator, target and the calibrated target.

**Configuration:** None

**Side Effects:** Inspection .json file. Diagnostics plots and data written to its directory.

### Shallow Clean

A shallow CLEAN is performed on the calibrated target MS to produce an initial (.fits) image. Uses WSClean.

**Configuration:** [ShallowCleanConfig][needle.config.clean.ShallowCleanConfig]

**Side Effects:** WSClean output .fits files

### Source Find & Masking

Finds sources in the shallow-cleaned image. Uses BANE for noise mapping and Aegean for the source finding algorithm.

Creates a (.fits) mask over the sources.

**Configuration:** [SourceFindConfig][needle.config.source_find.SourceFindConfig]

**Side Effects:** Noise map, source information (.json), source mask (.fits)

### Deep Clean

Performs a deep-clean on the image using the mask generated from the previous step.

**Configuration:** [DeepCleanConfig][needle.config.clean.DeepCleanConfig]

**Side Effects:** WSClean output .fits files

### Model Creation & Subtraction

Creates the `MODEL_DATA` column in the MS using WSClean's _predict_ functionality. Then performs another CLEAN step to subtract the model from the image.

**Configuration:** [DeepCleanConfig][needle.config.clean.DeepCleanConfig], [ModelSubtractCleanConfig][needle.config.clean.ModelSubtractCleanConfig]

**Side Effects:** .model.fits model file and the WSClean output .fits files. Updates the MS in place

### Interval Clean

Uses the previous MS inspection to determine the number of data intervals. Then performs a CLEAN on each of these intervals using the subtracted-model data.

**Configuration:** [IntervalCleanConfig][needle.config.clean.IntervalCleanConfig]

**Side Effects:** One .fits image for each interval of the dataset.
