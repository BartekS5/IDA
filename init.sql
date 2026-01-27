CREATE DATABASE mydb;
GO
USE mydb;
GO

CREATE TABLE users (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    auth_ref NVARCHAR(50),
    user_name NVARCHAR(50),
    password NVARCHAR(50),
    email NVARCHAR(50),
    name NVARCHAR(100),
    registered_at DATETIME,
    points INT,
    status NVARCHAR(20)
);

CREATE TABLE roles (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(50)
);

CREATE TABLE users_roles (
    user_id BIGINT,
    role_id BIGINT
);

CREATE TABLE points_action (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id BIGINT,
    action_type NVARCHAR(50),
    points INT,
    created_at DATETIME
);

CREATE TABLE restaurant (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    owner_id BIGINT,
    name NVARCHAR(100),
    cuisine NVARCHAR(50)
);

INSERT INTO users (auth_ref, user_name, password, email, name, registered_at, points, status)
VALUES ('auth0|123', 'jdoe', 'secret', 'jdoe@example.com', 'John Doe', GETDATE(), 100, 'active');

INSERT INTO roles (name) VALUES ('admin'), ('editor');
INSERT INTO users_roles (user_id, role_id) VALUES (1, 1), (1, 2);

INSERT INTO points_action (user_id, action_type, points, created_at)
VALUES (1, 'daily_login', 10, GETDATE());

INSERT INTO restaurant (owner_id, name, cuisine)
VALUES (1, 'The Burger Joint', 'American');
GO