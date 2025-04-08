#!/usr/bin/env python3
"""
This script demonstrates how to work with the "call_logs" table in Supabase.
It loads environment variables, connects to Supabase, inserts a test record into
the call_logs table, and then queries the table to verify the insertion.
"""

import os
import uuid  # Generate unique IDs for our records
from datetime import datetime, timezone  # Use timezone-aware datetime objects
from supabase import create_client, Client  # Supabase client library
from dotenv import load_dotenv  # To load environment variables from a .env file


def main():
    # Load environment variables from the .env file (make sure the .env file is in your project root)
    load_dotenv()

    # Retrieve your Supabase credentials from the environment variables.
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # Validate the Supabase credentials.
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "Missing SUPABASE_URL or SUPABASE_KEY in your .env file.")

    # Create a Supabase client instance.
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Prepare test data to be inserted into the "call_logs" table.
    # NOTE: In your real application, candidate_id should come from your candidates table.
    record_data = {
        # Generate a new unique UUID for the call log record.
        "id": str(uuid.uuid4()),
        # For testing, we use a generated UUID; replace this with a valid candidate ID.
        "candidate_id": str(uuid.uuid4()),
        # Dummy value representing the external (Twilio) call ID.
        "external_call_id": "test_call_sid",
        # Use a valid status value that your schema accepts.
        "status": "initiated",
        # Current time in UTC, ISO formatted.
        "started_at": datetime.now(timezone.utc).isoformat()
    }

    print("Attempting to insert the following data into call_logs:")
    print(record_data)

    # Insert the record into the call_logs table.
    insert_response = supabase.table("call_logs").insert(record_data).execute()

    # Print out the response from the insert action.
    print("Insert response data:", insert_response.data)
    print("Insert response error:", insert_response.error)

    # Now, query the table to verify the inserted record.
    # Here we select all columns, order by created_at descending, and limit to 1 record.
    query_response = supabase.table("call_logs").select(
        "*").order("created_at", desc=True).limit(1).execute()

    print("Query response data:", query_response.data)
    print("Query response error:", query_response.error)


if __name__ == "__main__":
    main()
