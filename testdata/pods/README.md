# Sample pod logs

`TestPodLogs` in `cmd/collector` runs the Collector on these files. They are in
the `/var/log/pods/<namespace>_<pod>_<uid>/<container>/0.log` layout, with the
container runtime's prefix on each line.

Most lines are copied from the recording in `../capture`:

- `cnpg-1/postgres`: an instance manager line, the instance manager's
  `pg_isready` check, logins and logouts of `included` and `excluded`, their
  wrong passwords, an unknown role, the end of a replica session, a new replica
  session and a checkpoint.
- `cnpg-1-initdb/initdb`: the initdb job up to its first login, with lines
  that are not PostgreSQL records.
- `cnpg-1/bootstrap-controller` and `noise/noise`: unrelated containers.
  `noise` logs plain text.

The recording has no failed login after authentication, so the lines at
22:29:00 and 22:29:01 in `cnpg-1/postgres` are written by hand, in the shape
that PostgreSQL 18 logged them in a lab: a valid certificate with the wrong CN,
no database, CONNECT denied, NOLOGIN and too many connections. They are the
same lines as in `internal/fixture`.
