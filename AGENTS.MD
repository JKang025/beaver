## Test layout
- Group tests under `test/` by the logical feature or component they cover; do not mirror every source subdirectory. (OTHER THAN GO, for which follow GO convention and keep tests within the same directory)
- Keep tests for nested helpers and utilities with the tests for their owning feature or component.
- Do not colocate tests with production source files.

## Comments
- Use brief comments to label the major stages of longer flows and make them easy to scan.
- Give functions a concise description when their purpose, behavior, side effects, or important quirks are not immediately obvious.
- Explain surprising logic, constraints, workarounds, and non-obvious decisions. Focus on why the code works this way.
- Do not restate what clear code already says or comment routine operations line by line.
- Prefer clear naming and structure over comments; use comments only where they add meaningful context.
- Keep comments concise and update them whenever the related behavior changes.