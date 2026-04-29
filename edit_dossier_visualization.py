#!/usr/bin/env python3
"""
Edits a visualization within a MicroStrategy Dossier (Dashboard) by replacing a metric.

This script demonstrates an advanced workflow that requires direct manipulation of the
Dossier's JSON definition. It uses mstrio-py for connection and object searching,
and the raw REST API for fetching and updating the dossier definition.

**Workflow:**
1. Connect to the MicroStrategy environment.
2. Define the target Dossier, visualization, and the metrics to be swapped.
3. Fetch the full JSON definition of the Dossier.
4. Locate the specific visualization within the JSON structure.
5. Find the metric to be replaced in the visualization's template and swap its ID.
6. Send the modified JSON definition back to the server to update the Dossier.

**Usage:**
- Update the placeholder IDs and names in the `main()` function.
- Run the script: `python edit_dossier_visualization.py`
"""

import os
import json
import logging
from pathlib import Path
from mstrio.connection import Connection
from mstrio.project_objects import list_metrics

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger("edit_dossier")


def _load_env() -> dict:
    """
    Loads environment variables from a .env file located in a parent directory.
    This makes the script more portable.
    """
    # Assumes the script is in /home/support/dev-projects/Scripts/
    # and the .env is in /home/support/dev-projects/Scripts/.env
    dotenv_path = Path(__file__).parent.parent / ".env"
    env = {}
    if not dotenv_path.exists():
        raise FileNotFoundError(f"Could not find .env file at expected location: {dotenv_path}")
    with open(dotenv_path, "rb") as f:
        for line in f:
            line = line.decode().strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k] = v.strip('"').strip("'")
    return env


class DossierEditor:
    """A class to handle the editing of a MicroStrategy Dossier definition."""

    def __init__(self, connection: Connection):
        """
        Initializes the DossierEditor with a mstrio Connection.

        Args:
            connection: An active mstrio.connection.Connection object.
        """
        self.conn = connection
        self.dossier_definition = None
        self.dossier_id = None

    def fetch_dossier_definition(self, dossier_id: str):
        """
        Fetches the full JSON definition of a dossier.

        Args:
            dossier_id (str): The object ID of the dossier to fetch.
        """
        LOG.info("Fetching definition for Dossier ID: %s", dossier_id)
        self.dossier_id = dossier_id
        endpoint = f"/api/v2/dossiers/{dossier_id}/definition"
        
        response = self.conn.get(endpoint=endpoint)
        if not response.ok:
            raise RuntimeError(f"Failed to fetch dossier definition: {response.text}")
        
        self.dossier_definition = response.json()
        LOG.info("Successfully fetched dossier definition.")

    def list_visualizations(self) -> list[dict]:
        """
        Parses the loaded dossier definition and returns a list of all visualizations.

        Returns:
            list[dict]: A list of dictionaries, each containing the 'key' and 'name'
                        of a visualization.
        """
        if not self.dossier_definition:
            raise ValueError("Dossier definition not fetched. Call fetch_dossier_definition() first.")

        viz_list = []
        LOG.info("Discovering visualizations in the dossier...")
        for i_ch, chapter in enumerate(self.dossier_definition.get('chapters', [])):
            for i_p, page in enumerate(chapter.get('pages', [])):
                for viz_key, visualization in page.get('visualizations', {}).items():
                    # The name can be in a few places; we'll check the most common ones.
                    viz_name = visualization.get('name') or visualization.get('title', {}).get('text')
                    viz_list.append({'key': viz_key, 'name': viz_name or f"Untitled Visualization (Chapter {i_ch+1}, Page {i_p+1})"})
        
        return viz_list

    def replace_metric_in_visualization(
        self, viz_key: str, old_metric_id: str, new_metric_id: str
    ) -> bool:
        """
        Finds a visualization by its key and replaces a metric in its template.

        Args:
            viz_key (str): The key of the visualization node (e.g., 'W62').
            old_metric_id (str): The object ID of the metric to be replaced.
            new_metric_id (str): The object ID of the new metric to add.

        Returns:
            bool: True if the metric was found and replaced, False otherwise.
        """
        if not self.dossier_definition:
            raise ValueError("Dossier definition not fetched. Call fetch_dossier_definition() first.")

        LOG.info("Searching for visualization with key: %s", viz_key)
        
        # Dossier definitions are structured by chapters and pages.
        # We need to iterate through them to find the visualization.
        for chapter in self.dossier_definition.get('chapters', []):
            for page in chapter.get('pages', []):
                if viz_key in page.get('visualizations', {}):
                    visualization = page['visualizations'][viz_key]
                    LOG.info("Found visualization '%s'. Searching for metric to replace...", viz_key)

                    # Metrics are typically found in the 'grid/template/metrics' array.
                    # The exact path can vary with visualization type.
                    metrics_list = visualization.get('grid', {}).get('template', {}).get('metrics', [])
                    
                    for i, metric_ref in enumerate(metrics_list):
                        if metric_ref.get('id') == old_metric_id:
                            LOG.info("Found metric ID %s at index %d. Replacing with %s.", old_metric_id, i, new_metric_id)
                            # Replace the ID
                            metric_ref['id'] = new_metric_id
                            # It's also good practice to update the name if it exists
                            if 'name' in metric_ref:
                                # In a real script, you'd look up the new metric's name
                                metric_ref['name'] = f"New Metric (ID: {new_metric_id})"
                            return True

        LOG.warning("Could not find visualization with key '%s' or metric ID '%s' in the definition.", viz_key, old_metric_id)
        return False

    def save_dossier(self):
        """
        Saves the modified dossier definition back to the MicroStrategy server.
        """
        if not self.dossier_definition or not self.dossier_id:
            raise ValueError("No dossier definition loaded or modified to save.")

        LOG.info("Saving modified definition for Dossier ID: %s", self.dossier_id)
        endpoint = f"/api/v2/dossiers/{self.dossier_id}/definition"
        
        headers = {
            'Content-Type': 'application/json',
            'X-MSTR-ProjectID': self.conn.project_id
        }

        response = self.conn.put(endpoint=endpoint, headers=headers, json=self.dossier_definition)

        if not response.ok:
            raise RuntimeError(f"Failed to save dossier definition: {response.text}")

        LOG.info("Dossier saved successfully.")

def find_metric_id_by_name(conn: Connection, name: str) -> str | None:
    """Helper function to find a metric's ID by its name."""
    LOG.info("Searching for metric with name: '%s'", name)
    try:
        # Using a search is more reliable than listing all metrics if there are many
        metrics = list_metrics(conn, name=name)
        if metrics:
            metric_id = metrics[0].id
            LOG.info("Found metric '%s' with ID: %s", name, metric_id)
            return metric_id
        else:
            LOG.warning("Metric with name '%s' not found.", name)
            return None
    except Exception as e:
        LOG.error("An error occurred while searching for metric '%s': %s", name, e)
        return None

def main():
    """Main function to execute the dossier editing workflow."""
    # --- Configuration ---
    # Replace these with your actual object IDs and names
    DOSSIER_ID = "E51138A411E9B85555820080EF552711"  # Example: Dossier ID to be edited
    METRIC_TO_REMOVE_NAME = "Revenue"
    METRIC_TO_ADD_NAME = "Profit"

    # --- Execution ---
    env = _load_env()
    conn = Connection(
        base_url=env["MSTR_BASE_URL"],
        username=env["MSTR_USERNAME"],
        password=env["MSTR_PASSWORD"],
        project_name=env.get("MSTR_PROJECT_NAME", "Shared Studio"),
        login_mode=int(env.get("MSTR_LOGIN_MODE", "1")),
    )

    try:
        # 1. Initialize the editor and fetch the dossier definition
        editor = DossierEditor(conn)
        editor.fetch_dossier_definition(DOSSIER_ID)

        # 2. List visualizations and prompt user for selection
        visualizations = editor.list_visualizations()
        if not visualizations:
            raise RuntimeError("No visualizations found in the dossier.")

        print("\nPlease select a visualization to edit:")
        for i, viz in enumerate(visualizations):
            print(f"  [{i}] {viz['name']} (Key: {viz['key']})")
        
        choice = int(input("\nEnter the number of the visualization: "))
        selected_viz = visualizations[choice]
        viz_key = selected_viz['key']
        LOG.info("User selected: '%s' (Key: %s)", selected_viz['name'], viz_key)

        # 3. Find metric IDs
        metric_to_remove_id = find_metric_id_by_name(conn, METRIC_TO_REMOVE_NAME)
        metric_to_add_id = find_metric_id_by_name(conn, METRIC_TO_ADD_NAME)

        if not all([metric_to_remove_id, metric_to_add_id]):
            raise ValueError("Could not find one or both metrics. Aborting.")

        # 4. Perform the replacement
        replaced = editor.replace_metric_in_visualization(viz_key, metric_to_remove_id, metric_to_add_id)

        # 5. Save if changes were made
        if replaced:
            editor.save_dossier()
            LOG.info("✅ Dossier editing complete.")
        else:
            LOG.warning("No changes were made to the dossier.")

    except (ValueError, RuntimeError) as e:
        LOG.error("❌ Operation failed: %s", e)
    finally:
        conn.close()
        LOG.info("Connection closed.")

if __name__ == "__main__":
    main()
