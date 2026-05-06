# MicroStrategy Unified CLI Tool (`mstr-cli`)

This project consolidates a collection of Python scripts into a single, robust command-line interface (CLI) tool for automating MicroStrategy operations, with a focus on Mosaic data models.

## 1. Objective

The primary goal is to provide a unified, maintainable, and user-friendly tool that replaces the scattered, single-purpose scripts. This refactoring effort centralizes common logic like authentication, configuration, and API interactions into a shared library.

For more details on the refactoring strategy, see plan.md.

## 2. Prerequisites

-   Python 3.9+
-   Required Python packages as listed in `requirements.txt`.

## 3. Setup

### Environment Variables

The CLI tool requires a `.env` file in the project's root directory (`/home/support/dev-projects/Scripts/.env`) for database credentials and connection details.

Create the `.env` file with the following content:

```env
# .env

MSTR_BASE_URL="https://your-mstr-environment.com/MicroStrategyLibrary"
MSTR_USERNAME="your_username"
MSTR_PASSWORD="your_password"
MSTR_PROJECT_NAME="Your Project Name"
MSTR_LOGIN_MODE="1" # 1 for Standard, 16 for LDAP
```

### Python Dependencies

Install the necessary packages using pip:

```bash
pip install -r requirements.txt
```

## 4. Usage

The tool is executed through the `main.py` entry point. The basic structure of a command is:

```bash
python -m mstr_cli.main <command-group> <command> [options]
```

### Global Options

-   `--project <name>`: Overrides the `MSTR_PROJECT_NAME` from the `.env` file.

### Available Commands

#### `model`

Creates a new Mosaic data model from one or more database tables. This command is a port of the `01_create_mosaic_model.py` script.

**Arguments:**

-   `--model-name`: (Required) The name for the new model.
-   `--source-config`: A JSON file defining multiple source tables.
-   `--datasource`: The datasource name (for single-source mode).
-   `--table`: The table name (for single-source mode).
-   `--schema`: The database schema (defaults to `public`).
-   `--date-columns`: Comma-separated columns to cast as date type.
-   `--description`: A description for the model.
-   `--folder-id`: The ID of the destination folder.
-   `--keep-workspace`: If set, the temporary data import workspace will not be deleted after the model is created.

**Example (Single Source):**

```bash
python -m mstr_cli.main model create \
    --model-name "My New CLI Model" \
    --datasource "glagrange - Postgresql" \
    --table "my_sales_data"
```