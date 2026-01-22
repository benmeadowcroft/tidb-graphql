CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL
);

CREATE TABLE posts (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  title VARCHAR(200) NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE user_analytics (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  metric VARCHAR(100) NOT NULL,
  value INT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE audit_logs (
  id INT PRIMARY KEY,
  action VARCHAR(200) NOT NULL,
  created_at DATETIME NOT NULL
);
