type conn

val connect : ip:string -> port:int -> conn Lwt.t

val query : conn -> query:string -> ?values:Scylla.value array -> unit -> Scylla.rows Lwt.t
