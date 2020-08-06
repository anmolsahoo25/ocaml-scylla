type t
(** Type of a scylla connection *)

type value = Protocol.value
(** Type of values stored in the database *)

val show_value : value -> string
(** Pretty-printer for values *)

val connect : ip:string -> port:int -> (t, string) result
(** Connect to a scylla node *)

val query : t -> query:string -> (value array array, string) result
(** Execute a sync query, returning an array of rows *)
