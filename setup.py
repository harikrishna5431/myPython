# setup.py
import sqlite3
import hashlib
from main import get_app_db_connection, init_app_db

def create_user(username, password, role, email):
    """Create a new user in the system"""
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    cursor.execute(
        """
        INSERT OR REPLACE INTO users (username, password_hash, role, email)
        VALUES (?, ?, ?, ?)
        """,
        (username, password_hash, role, email)
    )
    
    conn.commit()
    conn.close()

def add_db_connection(env_name, connection_string, description):
    """Add a new database connection to the db_connections table."""
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        """
        INSERT OR REPLACE INTO db_connections (env_name, connection_string, description)
        VALUES (?, ?, ?)
        """,
        (env_name, connection_string, description)
    )
    
    conn.commit()
    conn.close()

def main():
    # Initialize the database schema
    init_app_db()
    
    # Add some test users
    create_user("requestor1", "password123", "requestor", "requestor1@example.com")
    create_user("manager1", "password123", "manager", "manager1@example.com")
    create_user("support1", "password123", "support", "support1@example.com")
    
    # Set up database connections
    add_db_connection("DEV", "postgresql://user:password@dev-server:5432/database", "Development Database")
    add_db_connection("UAT", "postgresql://user:password@uat-server:5432/database", "User Acceptance Testing")
    add_db_connection("PROD", "postgresql://user:password@prod-server:5432/database", "Production Database")
    
    print("Setup complete!")

if __name__ == "__main__":
    main()