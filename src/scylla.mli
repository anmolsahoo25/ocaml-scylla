type t
(** Type of a scylla connection *)

type value = Protocol.value
(** Type of values stored in the database *)

val connect : ip:string -> port:int -> t
(** Connect to a scylla node *)

val query : t -> query:string -> value array array
(** Execute a sync query, returning an array of rows *)
