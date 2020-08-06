type t
(** Type of a scylla connection *)

type bigstring = Bigstringaf.t
(** Type alias for bigstrings *)

type value = Protocol.value
(** Type of values stored in the database *)

type rows = { table_spec : (bigstring * bigstring * bigstring) array ; values: value array array }
(** Type of rows returned by query / execute statements *)

val show_value : value -> string
(** Pretty-printer for values *)

val connect : ip:string -> port:int -> (t, string) result
(** Connect to a scylla node *)

val query : t -> query:string -> ?values:value array -> unit -> (rows, string) result
(** Execute a sync query, returning an array of rows *)
