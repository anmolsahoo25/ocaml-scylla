type conn
(** Type of a scylla connection *)

type bigstring = Bigstringaf.t
(** Type alias for bigstrings *)

type value = Protocol.value
(** Type of values stored in the database *)

type rows = { table_spec : (bigstring * bigstring * bigstring) array ; values: value array array }
(** Type of rows returned by query / execute statements *)

val show_value : value -> string
(** Pretty-printer for values *)

val connect : ip:string -> port:int -> (conn, string) result
(** Connect to a scylla node *)

val query : conn -> query:string -> ?values:value array -> unit -> (rows, string) result
(** Execute a sync query, returning an array of rows *)

module Protocol = Protocol
(** CQL v4 protocol definitions *)

module Serialize = Serialize
(** Serializers for protocol packets *)

module Parse = Parse
(** Parsers for protocol packets *)
