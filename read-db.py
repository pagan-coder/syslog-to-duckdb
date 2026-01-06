#!/usr/bin/env python3
"""
A script that reads and displays Syslog entries from DuckDB database.
"""

import sys
import argparse

import duckdb


def read_syslog_db(db_path, table_name, limit=None):
    """
    Read syslog entries from DuckDB database and display them.
    
    Args:
        db_path: Path to DuckDB database file
        table_name: Name of the table to read from
        limit: Optional limit on number of rows to display
    """
    try:
        # Connect to DuckDB database
        con = duckdb.connect(db_path, read_only=True)
        
        # Build query
        query = f"SELECT * FROM {table_name} ORDER BY timestamp DESC"
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        result = con.execute(query).fetchall()
        
        # Get column names
        columns = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        column_names = [col[1] for col in columns]
        
        # Print header
        print(f"\n{'='*100}")
        print(f"Found {len(result)} entries in table '{table_name}'")
        print(f"{'='*100}\n")
        
        # Print column headers
        header = " | ".join(f"{col:20}" for col in column_names)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in result:
            row_str = " | ".join(f"{str(val):20}" for val in row)
            print(row_str)
        
        print(f"\n{'='*100}\n")
        
        # Close connection
        con.close()
        
    except duckdb.CatalogException as e:
        print(f"Error: Table '{table_name}' not found in database.", file=sys.stderr)
        print(f"Make sure the table exists and the name is correct.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading database: {e}", file=sys.stderr)
        sys.exit(1)


def list_tables(db_path):
    """List all tables in the database."""
    try:
        con = duckdb.connect(db_path, read_only=True)
        tables = con.execute("SHOW TABLES").fetchall()
        con.close()
        
        if tables:
            print("Tables in database:")
            for table in tables:
                print(f"  - {table[0]}")
        else:
            print("No tables found in database.")
    except Exception as e:
        print(f"Error listing tables: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Read and display syslog entries from DuckDB database"
    )
    parser.add_argument(
        "--db",
        default="syslog.db",
        help="Path to DuckDB database file (default: syslog.db)"
    )
    parser.add_argument(
        "--table",
        default="syslog_logs",
        help="Table name to read from (default: syslog_logs)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of rows to display"
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all tables in the database"
    )
    
    args = parser.parse_args()
    
    if args.list_tables:
        list_tables(args.db)
    else:
        read_syslog_db(args.db, args.table, args.limit)


if __name__ == "__main__":
    main()
