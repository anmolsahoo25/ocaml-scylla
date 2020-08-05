Scylla/Cassandra driver written in pure OCaml

## Quickstart
Assuming a table `keyspace1.table` with schema `(id text, person text)`
exists with the only value `(id1, person1)` then -

```
let _ =
  let conn = Scylla.connect ~ip:"172.17.0.1" ~port:9042 in
  let rows = Scylla.query conn "select * from keyspace1.person" in
  assert ((Bigstring.to_string rows.(0).(0)) = "id1") ;
  assert ((Bigstring.to_string rows.(0).(1)) = "person1")
```
