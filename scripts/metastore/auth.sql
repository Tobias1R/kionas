
/*
Initialize the metastore database with authentication tables and a default user.

This script only create the authentication db and tables, and inserts a 
default user with username 'kionas' and password 'kionas'.
*/

-- Create the authentication database
CREATE DATABASE IF NOT EXISTS kionas_auth;
-- Use the authentication database
\c kionas_auth;
-- Create the users table to store user credentials
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    can_login BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert a default user
INSERT INTO users (username, password_hash, can_login) VALUES ('kionas', 'kionas', TRUE);

