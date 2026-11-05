# Classic reference journal fixture

Monitor writes a Reading, publishes its exact version and synchronously triggers
Analysis. Analysis reads the same version and returns Nominal. Monitor then
returns Monitored. Settings is a setup write.

The two Knowledge records store each value once; Phase records cite references.
The callback Write and Knowledge commit are atomic and share an admission time.
At equal times the merged file places Knowledge before its Phase reference.
Admission times and unique activation IDs are normalised for fixture comparison.

Regenerate with `PYTHONPATH=. python tests/observation_nominal.py PATH` and use the
normalisation in `tests/test_observation.py` before committing.
