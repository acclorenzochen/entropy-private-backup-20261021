"""V3 standalone policy, execution, and audited host building blocks.

These modules do not import or inherit the legacy Engine. The default host mode
is read-only; live entry remains disabled until its explicit release gates pass.
Running the demo uses deterministic fake venues.
"""
