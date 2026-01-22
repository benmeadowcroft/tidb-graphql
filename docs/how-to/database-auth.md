# Authenticate to TiDB

Goal: provide the database password securely when starting the server.

## 1) Password prompt (recommended)

```bash
./bin/tidb-graphql --database.password_prompt
```

This avoids passwords in process lists or environment variables.

## 2) Password file

Create a file with just the password (no newline preferred):

```bash
printf '%s' 'your_password' > /path/to/db.password
```

Then reference it:

```yaml
database:
  password_file: /path/to/db.password
```

## 3) Read from stdin with @-

This is useful when piping from a password manager or another command.

```bash
echo 'your_password' | ./bin/tidb-graphql --database.password=@-
```

Example using 1Password CLI:

```bash
op read "op://MyVault/TiDB/password" | ./bin/tidb-graphql --database.password=@-
```

## 4) Environment variable

```bash
export TIGQL_DATABASE_PASSWORD='your_password'
./bin/tidb-graphql
```

Use this only if you are comfortable with the value living in your environment.

## 5) Plain CLI flag (least secure)

```bash
./bin/tidb-graphql --database.password='your_password'
```

This can show up in process listings. Prefer the prompt or file options.

---
# Related Docs

## Next steps
- [DB user for roles](db-user-for-roles.md)
- [Configure OIDC/JWKS auth](oidc-jwks-auth.md)

## Reference
- [Auth reference](../reference/auth.md)
- [Configuration reference](../reference/configuration.md)

## Further reading
- [Database-first authorization](../explanation/database-first-auth.md)

## Back
- [How-to home](README.md)
- [Docs home](../README.md)
