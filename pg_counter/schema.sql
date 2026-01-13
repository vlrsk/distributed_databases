DROP TABLE IF EXISTS user_counter;

CREATE TABLE user_counter (
  user_id  INT PRIMARY KEY,
  counter  INT NOT NULL,
  version  INT NOT NULL
);

INSERT INTO user_counter(user_id, counter, version) VALUES (1, 0, 0)
ON CONFLICT (user_id) DO UPDATE SET counter = 0, version = 0;
