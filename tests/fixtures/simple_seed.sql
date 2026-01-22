-- Seed data for simple schema

INSERT INTO users (id, username, email) VALUES
    (1, 'alice', 'alice@example.com'),
    (2, 'bob', 'bob@example.com'),
    (3, 'charlie', 'charlie@example.com');

INSERT INTO posts (id, user_id, title, content, published) VALUES
    (1, 1, 'First Post', 'This is Alice''s first post', TRUE),
    (2, 1, 'Second Post', 'Alice''s second post', TRUE),
    (3, 2, 'Bob''s Post', 'A post from Bob', FALSE),
    (4, 3, 'Charlie''s Adventures', 'Charlie writes about adventures', TRUE);

INSERT INTO comments (post_id, user_id, comment_text) VALUES
    (1, 2, 'Great post, Alice!'),
    (1, 3, 'I enjoyed reading this.'),
    (2, 2, 'Another excellent post!'),
    (4, 1, 'Wow, amazing adventures!'),
    (4, 2, 'I want to go there too!');
