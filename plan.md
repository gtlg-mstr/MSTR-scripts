# Plan: Consolidate MicroStrategy Scripts into a Unified CLI Tool

This document outlines a plan to refactor the existing collection of Python scripts into a single, robust command-line interface (CLI) tool. The goal is to improve usability, maintainability, and consistency.

## 1. Project Status

**As of 2026-05-03, the core application structure is in place and the first command (`model create`) has been successfully migrated.**

## 2. Core Objectives

-   **Unified Interface**: Create a single entry point (`mstr-cli`) for all operations.
-   **Command-based Structure**: Use a subcommand structure (e.g., `mstr-cli model create`, `mstr-cli dossier edit`).
-   **Code Consolidation**: Centralize common logic like authentication, environment loading, and API interactions into a shared library.
-   **Consistent Configuration**: Standardize argument parsing, logging, and configuration across all commands.
-   **Maintainability**: Make it easier to add, update, and debug functionality.

## 3. Proposed CLI Structure

We will use a library like `argparse` or a more advanced one like `click` or `typer` to build the CLI. The structure will be based on subcommands grouped by the object they operate on (e.g., `model`, `dossier`, `metric`).

```
mstr-cli
│
├── model
│   ├── create (from 01_create_mosaic_model.py / archive/create_mosaic_model_v2.py)
│   ├── add-column (from add_mosaic_column_v2.1.py)
│   ├── create-metric (from 03_create_mosaic_metric.py)
│   ├── delete-attribute (from 08_delete_mosaic_attribute.py)
│   └── delete-metric (from 09_delete_mosaic_metric.py)
│
├── dossier
│   └── edit (from 07_edit_dossier_visualization.py)
│
└── dictionary
    └── generate (from 04_mstr_data_dictionary.py)
```

## 3. Refactoring and Implementation Plan

### Step 1: Create the Core Application Structure

1.  **Create a `mstr_cli` package**:
    ```
    mstr_cli/
    ├── __init__.py
    ├── main.py         # CLI entry point
    ├── core/           # Shared logic
    │   ├── __init__.py
    │   ├── auth.py     # Connection and session management
    │   ├── config.py   # .env and config file loading
    │   └── utils.py    # Common helpers (logging setup, etc.)
    └── commands/       # Subcommand modules
        ├── __init__.py
        ├── model.py
        ├── dossier.py
        └── dictionary.py
    ```

2.  **Implement `core/config.py`**:
    -   Create a function `load_environment()` that reliably finds and parses the `.env` file. This will replace the `_load_env()` function duplicated across many scripts.

3.  **Implement `core/auth.py`**:
    -   Create a `MstrClient` class that encapsulates the `mstrio.connection.Connection` object.
    -   This class will handle login, logout, and provide methods for making raw REST API calls (`get`, `post`, `put`, `delete`) while managing headers (auth token, project ID, changeset ID).
    -   It will absorb the logic from the `MSTRSession` class in `01_create_mosaic_model.py` and `02_add_mosaic_column.py`.

### Step 2: Port Existing Scripts to Commands

For each numbered script, we will create a corresponding function in the appropriate `commands/` module.

#### `01_create_mosaic_model.py` -> `commands/model.py`

-   **Function**: `create_model(...)`
-   **Action**: Move the core logic from `_run()` into this function.
-   **Refactor**:
    -   Replace `MSTRSession` with the new `MstrClient` from `core/auth.py`.
    -   The extensive helper functions (`create_pipeline_phase1`, `add_table_to_model`, etc.) should be kept within the `commands/model.py` module, as they are specific to this complex workflow.
    -   The `argparse` logic will be handled by the main CLI entry point.

#### `07_edit_dossier_visualization.py` -> `commands/dossier.py`

-   **Function**: `edit_visualization(...)`
-   **Action**: Port the `main()` workflow into this function. The `DossierEditor` class can be moved into this module as a helper.
-   **Refactor**:
    -   Replace direct `mstrio.connection.Connection` instantiation with the shared `MstrClient`.
    -   The interactive prompt for selecting a visualization will be preserved. The command will take `dossier-id`, `metric-to-remove`, and `metric-to-add` as arguments.

#### `04_mstr_data_dictionary.py` -> `commands/dictionary.py`

-   **Function**: `generate_dictionary(...)`
-   **Action**: Move the logic from `main()` into this function.
-   **Refactor**:
    -   The helper functions (`_normalize_type`, `fetch_model_columns`, etc.) will be co-located in `commands/dictionary.py`.
    -   Replace direct connection logic with the shared `MstrClient`.

#### `09_delete_mosaic_metric.py` -> `commands/model.py`

-   **Function**: `delete_metric(...)`
-   **Action**: Port the `run()` function's logic.
-   **Refactor**:
    -   Use the shared `MstrClient` for connection and API calls.
    -   The helper functions (`create_changeset`, `commit_changeset`, `find_metric`) can be generalized and moved to `core/utils.py` if they are used by other model commands.

#### `02_add_mosaic_column.py` (and `add_mosaic_column_v2.1.py`) -> `commands/model.py`

-   **Function**: `add_column(...)`
-   **Action**: Use the logic from the working script (`add_mosaic_column_v2.1.py`).
-   **Refactor**:
    -   Replace `MSTRSession` with the shared `MstrClient`.
    -   The payload builders and AI inference logic will be moved into this command function or its helpers.

### Step 3: Build the Main CLI Entry Point (`main.py`)

1.  Use `argparse` or a similar library to define the main parser and subparsers for each command (`model`, `dossier`, `dictionary`).
2.  For each subcommand, define its arguments (e.g., `--model-id`, `--table-name`).
3.  The main function will parse the arguments, instantiate the `MstrClient`, and call the appropriate command function with the parsed arguments.
4.  Implement global flags like `--verbose` or `--project` that can be handled at the top level.