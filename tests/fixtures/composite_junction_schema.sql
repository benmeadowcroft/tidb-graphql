DROP TABLE IF EXISTS project_memberships;
DROP TABLE IF EXISTS user_groups;
DROP TABLE IF EXISTS projects;
DROP TABLE IF EXISTS `groups`;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    tenant_id INT NOT NULL,
    id INT NOT NULL,
    username VARCHAR(100) NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE `groups` (
    tenant_id INT NOT NULL,
    id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE projects (
    tenant_id INT NOT NULL,
    id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

-- Pure junction (many-to-many) with composite foreign keys on both sides.
CREATE TABLE user_groups (
    tenant_id INT NOT NULL,
    user_id INT NOT NULL,
    group_tenant_id INT NOT NULL,
    group_id INT NOT NULL,
    CONSTRAINT fk_user_groups_user
        FOREIGN KEY (tenant_id, user_id) REFERENCES users (tenant_id, id),
    CONSTRAINT fk_user_groups_group
        FOREIGN KEY (group_tenant_id, group_id) REFERENCES `groups` (tenant_id, id),
    PRIMARY KEY (tenant_id, user_id, group_tenant_id, group_id)
);

-- Attribute junction (edge list) with composite foreign keys on both sides.
CREATE TABLE project_memberships (
    tenant_id INT NOT NULL,
    user_id INT NOT NULL,
    project_tenant_id INT NOT NULL,
    project_id INT NOT NULL,
    role_level VARCHAR(32) NOT NULL,
    assigned_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_project_memberships_user
        FOREIGN KEY (tenant_id, user_id) REFERENCES users (tenant_id, id),
    CONSTRAINT fk_project_memberships_project
        FOREIGN KEY (project_tenant_id, project_id) REFERENCES projects (tenant_id, id),
    PRIMARY KEY (tenant_id, user_id, project_tenant_id, project_id)
);
