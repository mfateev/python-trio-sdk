"""Bridge integration pattern tests.

These tests verify SDK-Core behavior for workflow features by directly
interacting with the bridge layer. Each pattern demonstrates:
- What activation jobs are received
- What completion commands are expected
- Field requirements and formats
- Edge cases

Test Organization:
- test_activities.py: Patterns 8-10 (Activity execution, failure, cancellation)
- test_signals_queries.py: Patterns 11-13 (Signals and queries)
- test_child_workflows.py: Patterns 14-15 (Child workflows)
- test_advanced.py: Patterns 16-19 (Failure, continue-as-new, etc.)

Prerequisites:
- Temporal server running on localhost:7233
- Rust bridge built with `maturin develop --release`
"""
