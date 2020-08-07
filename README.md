Scylla/Cassandra driver written in pure OCaml

# WORK-IN-PROGRESS
This driver is not complete, stable and completely tested. Will
be posting updates as I feel it uses a reachable state.

## Quickstart
Assuming a table `keyspace1.table` with schema `(id text, person text)`
exists with the only value `(id1, person1)` then -

```
open Result
open Scylla

let _ =
  let query_string = "select * from keyspace1.person" in
  let conn = connect ~ip:"172.17.0.2" ~port:9042 |> get_ok in
  let values = query conn ~query:query_string |> get_ok in
  let print_row r =
    Printf.printf "%s, %s\n" (show_value r.(0)) (show_value r.(1)) in
  Array.iter print_row values
```
