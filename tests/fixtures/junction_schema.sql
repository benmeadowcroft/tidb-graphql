-- Junction table test schema
-- Tests both pure junctions (only FK columns) and attribute junctions (extra columns)

DROP TABLE IF EXISTS user_roles;
DROP TABLE IF EXISTS project_members;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS roles;
DROP TABLE IF EXISTS projects;

-- Base tables
CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY (email)
);

CREATE TABLE roles (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    description VARCHAR(255),
    PRIMARY KEY (id),
    UNIQUE KEY (name)
);

CREATE TABLE projects (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Pure junction: only FK columns, should generate direct M2M fields
-- and hide the junction table type
CREATE TABLE user_roles (
    user_id INT NOT NULL,
    role_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    FOREIGN KEY (role_id) REFERENCES roles (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

-- Attribute junction: has extra columns (assigned_at, role_level)
-- Should generate edge type ProjectMember with these attributes
CREATE TABLE project_members (
    user_id INT NOT NULL,
    project_id INT NOT NULL,
    assigned_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    role_level ENUM('viewer', 'editor', 'admin') NOT NULL DEFAULT 'viewer',
    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, project_id)
);

-- Insert sample data
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.com');

INSERT INTO roles (name, description) VALUES
    ('admin', 'System administrator'),
    ('editor', 'Content editor'),
    ('viewer', 'Read-only access');

INSERT INTO projects (name, description) VALUES
    ('Project Alpha', 'First project'),
    ('Project Beta', 'Second project');

-- User-role assignments (pure junction)
INSERT INTO user_roles (user_id, role_id) VALUES
    (1, 1), -- Alice is admin
    (1, 2), -- Alice is also editor
    (2, 2), -- Bob is editor
    (3, 3); -- Charlie is viewer

-- Project membership (attribute junction)
INSERT INTO project_members (user_id, project_id, role_level) VALUES
    (1, 1, 'admin'),   -- Alice is admin on Alpha
    (1, 2, 'viewer'),  -- Alice is viewer on Beta
    (2, 1, 'editor'),  -- Bob is editor on Alpha
    (2, 2, 'admin');   -- Bob is admin on Beta
