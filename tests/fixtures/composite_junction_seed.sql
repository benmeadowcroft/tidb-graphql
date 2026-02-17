INSERT INTO users (tenant_id, id, username) VALUES
    (1, 1, 'alice_t1'),
    (2, 1, 'alice_t2'),
    (1, 2, 'bob_t1');

INSERT INTO `groups` (tenant_id, id, name) VALUES
    (1, 10, 'admins_t1'),
    (2, 10, 'admins_t2'),
    (1, 11, 'devs_t1');

INSERT INTO projects (tenant_id, id, name) VALUES
    (1, 100, 'apollo_t1'),
    (2, 100, 'apollo_t2');

INSERT INTO user_groups (tenant_id, user_id, group_tenant_id, group_id) VALUES
    (1, 1, 1, 10),
    (2, 1, 2, 10),
    (1, 2, 1, 11);

INSERT INTO project_memberships (tenant_id, user_id, project_tenant_id, project_id, role_level) VALUES
    (1, 1, 1, 100, 'owner'),
    (2, 1, 2, 100, 'viewer'),
    (1, 2, 1, 100, 'editor');
