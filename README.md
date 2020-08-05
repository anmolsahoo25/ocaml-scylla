Scylla/Cassandra driver written in pure OCaml

## Quickstart
```
(* assuming - keyspace1.table(id text, person text)
   with value ("id1", "person1") *)
let _ =
  let conn = Scylla.connect ~ip:"172.17.0.1" ~port:9042 in
  let rows = Scylla.query conn "select * from keyspace1.person" in
  assert ((Bigstring.to_string rows.(0).(0)) = "id1") ;
  assert ((Bigstring.to_string rows.(0).(1)) = "person1")
```
