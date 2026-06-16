"""
Database setup and initialization script
"""
from os import getenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import logging

from database import DatabaseManager

def create_database_if_not_exists():
    """Create the PostgreSQL database if it doesn't exist"""
    load_dotenv()
    
    db_host = getenv('DB_HOST', 'localhost')
    db_port = getenv('DB_PORT', '5432')
    db_name = getenv('DB_NAME', 'options_data')
    db_user = getenv('DB_USER', 'postgres')
    db_password = getenv('DB_PASSWORD', '')
    
    # Connect to PostgreSQL server (not specific database)
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database='postgres'  # Connect to default postgres database
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Created database: {db_name}")
        else:
            print(f"Database {db_name} already exists")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def initialize_tables():
    """Initialize database tables"""
    try:
        db_manager = DatabaseManager()
        db_manager.create_tables()
        print("Database tables created successfully")
        db_manager.close()
        return True
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False

def main():
    """Main setup function"""
    print("Setting up Options Data Manager database...")
    
    # Create database
    if not create_database_if_not_exists():
        print("Failed to create database")
        return False
    
    # Create tables
    if not initialize_tables():
        print("Failed to create tables")
        return False
    
    print("Database setup completed successfully!")
    return True

if __name__ == "__main__":
    main()