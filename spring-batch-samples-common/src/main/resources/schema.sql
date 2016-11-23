create table if not exists users (
  id INT NOT NULL,
  name VARCHAR(100) NOT NULL,
  position VARCHAR(100) NOT NULL,
  companyNumber VARCHAR(10) NOT NULL
);

create table if not exists mailingList (
  id INT NOT NULL,
  email VARCHAR(100) NOT NULL
);