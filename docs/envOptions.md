# Environment Variable Options

- `CONTAINER_LOG_TAIL`: Specifies the number of lines to display from container logs. Defaults to 50 lines.

- `ICTEST_CONFIGURED_CHAINS`: overrides the default configuredChains.yaml embedded config.

- `ICTEST_DEBUG`: extra debugging information for test execution.

- `ICTEST_HOME`: The folder to use as the home / working directory.

- `ICTEST_SKIP_FAILURE_CLEANUP`: skips cleanup of the temporary directory on a test failure.

- `KEEP_CONTAINERS`: Prevents testnet cleanup after completion.

    - Set to any non-empty value to keep testnet containers alive.

- `SHOW_CONTAINER_LOGS`: Controls whether container logs are displayed.

    - Set to `"always"` to show logs for both pass and fail.
    - Set to `"never"` to never show any logs.
    - Leave unset to show logs only for failed tests.

- `ICS_SPAWN_TIME_WAIT`: A go duration (e.g. 2m) that specifies how long to wait for ICS chains to spawn
