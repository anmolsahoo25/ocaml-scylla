type t = {ic : in_channel ; oc : out_channel}

type value = Protocol.value

let connect ~ip:ip ~port:port =
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string ip, port) in
  let (ic, oc) = Unix.open_connection addr in
  (* let startup = Protocol.(Req { flags = [] ; stream = 0; op = Startup ; body = Empty }) in
  let startup_bytes = Serialize.(to_bytes startup) *)
  {ic ; oc}

let query _conn ~query:(_query) =
  [||]
