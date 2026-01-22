INSERT INTO users (id, name) VALUES
  (1, 'Alice'),
  (2, 'Bob');

INSERT INTO posts (id, user_id, title) VALUES
  (1, 1, 'Hello World'),
  (2, 2, 'Second Post');

INSERT INTO user_analytics (id, user_id, metric, value) VALUES
  (1, 1, 'login_count', 3),
  (2, 2, 'login_count', 5);

INSERT INTO audit_logs (id, action, created_at) VALUES
  (1, 'user_login', '2024-01-01 10:00:00'),
  (2, 'post_create', '2024-01-02 11:00:00');
