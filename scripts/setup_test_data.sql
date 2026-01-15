-- Setup test data for migration testing

USE mydb;
GO

-- Create tables if they don't exist
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users')
BEGIN
    CREATE TABLE users (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        auth_ref VARCHAR(255),
        user_name VARCHAR(100) NOT NULL,
        password VARCHAR(100) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(100),
        registered_at DATETIMEOFFSET,
        points INT DEFAULT 0,
        status VARCHAR(50) DEFAULT 'ACTIVE'
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'roles')
BEGIN
    CREATE TABLE roles (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users_roles')
BEGIN
    CREATE TABLE users_roles (
        user_id BIGINT NOT NULL,
        role_id BIGINT NOT NULL,
        PRIMARY KEY (user_id, role_id),
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'points_action')
BEGIN
    CREATE TABLE points_action (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        user_id BIGINT NOT NULL,
        action_type VARCHAR(50),
        points INT,
        timestamp DATETIMEOFFSET,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'restaurant')
BEGIN
    CREATE TABLE restaurant (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        name VARCHAR(255),
        owner_id BIGINT,
        FOREIGN KEY (owner_id) REFERENCES users(id)
    );
END
GO

-- Insert sample roles
IF NOT EXISTS (SELECT * FROM roles WHERE name = 'ADMIN')
    INSERT INTO roles (name) VALUES ('ADMIN');
IF NOT EXISTS (SELECT * FROM roles WHERE name = 'USER')
    INSERT INTO roles (name) VALUES ('USER');
IF NOT EXISTS (SELECT * FROM roles WHERE name = 'MANAGER')
    INSERT INTO roles (name) VALUES ('MANAGER');
GO

-- Insert sample users
INSERT INTO users (auth_ref, user_name, password, email, name, registered_at, points, status)
VALUES 
    ('auth|123456', 'john_doe', 'hashed_password_1', 'john@example.com', 'John Doe', SYSDATETIMEOFFSET(), 150, 'ACTIVE'),
    ('auth|789012', 'jane_smith', 'hashed_password_2', 'jane@example.com', 'Jane Smith', SYSDATETIMEOFFSET(), 200, 'ACTIVE'),
    ('auth|345678', 'bob_wilson', 'hashed_password_3', 'bob@example.com', 'Bob Wilson', SYSDATETIMEOFFSET(), 75, 'INACTIVE');
GO

-- Assign roles to users
DECLARE @john_id BIGINT = (SELECT id FROM users WHERE user_name = 'john_doe');
DECLARE @jane_id BIGINT = (SELECT id FROM users WHERE user_name = 'jane_smith');
DECLARE @bob_id BIGINT = (SELECT id FROM users WHERE user_name = 'bob_wilson');
DECLARE @admin_role BIGINT = (SELECT id FROM roles WHERE name = 'ADMIN');
DECLARE @user_role BIGINT = (SELECT id FROM roles WHERE name = 'USER');
DECLARE @manager_role BIGINT = (SELECT id FROM roles WHERE name = 'MANAGER');

INSERT INTO users_roles (user_id, role_id) VALUES
    (@john_id, @admin_role),
    (@john_id, @user_role),
    (@jane_id, @user_role),
    (@jane_id, @manager_role),
    (@bob_id, @user_role);
GO

-- Insert points actions
DECLARE @john_id BIGINT = (SELECT id FROM users WHERE user_name = 'john_doe');
DECLARE @jane_id BIGINT = (SELECT id FROM users WHERE user_name = 'jane_smith');

INSERT INTO points_action (user_id, action_type, points, timestamp) VALUES
    (@john_id, 'EARNED', 50, DATEADD(day, -5, SYSDATETIMEOFFSET())),
    (@john_id, 'EARNED', 100, DATEADD(day, -3, SYSDATETIMEOFFSET())),
    (@john_id, 'SPENT', -20, DATEADD(day, -1, SYSDATETIMEOFFSET())),
    (@jane_id, 'EARNED', 150, DATEADD(day, -7, SYSDATETIMEOFFSET())),
    (@jane_id, 'EARNED', 50, DATEADD(day, -2, SYSDATETIMEOFFSET()));
GO

-- Insert restaurants
DECLARE @john_id BIGINT = (SELECT id FROM users WHERE user_name = 'john_doe');
DECLARE @jane_id BIGINT = (SELECT id FROM users WHERE user_name = 'jane_smith');

INSERT INTO restaurant (name, owner_id) VALUES
    ('The Golden Fork', @john_id),
    ('Sunset Bistro', @john_id),
    ('Garden Cafe', @jane_id);
GO

PRINT 'Test data setup completed successfully!';
