open Protocol
open Angstrom

let header = char '\x04' <|> char '\x84'

let flags = any_char >>= fun _ -> return []

let stream = BE.any_int16

let op =
  any_char >>= fun c ->
  let op =
    match c with
    | '\x00' -> Error
    | '\x01' -> Startup
    | '\x02' -> Ready
    | '\x03' -> Authenticate
    | '\x05' -> Options
    | '\x06' -> Supported
    | '\x07' -> Query
    | '\x08' -> Result
    | '\x09' -> Prepare
    | '\x0a' -> Execute
    | '\x0b' -> Register
    | '\x0c' -> Event
    | '\x0d' -> Batch
    | '\x0e' -> Auth_challenge
    | '\x0f' -> Auth_response
    | '\x10' -> Auth_success
    | _ -> failwith "unknown char"
  in
  return op

let len = BE.any_int32

let string =
  BE.any_uint16 >>= fun n -> take_bigstring n

let string_pair =
  string >>= fun s1 ->
  string >>= fun s2 ->
  return (s1,s2)

let string_map =
  BE.any_uint16 >>= fun n -> count n string_pair

let body op =
  match op with
  | Options -> string_map >>= fun l -> return (Map l)
  | _ -> return Empty

let parser =
  header >>= fun c ->
  flags >>= fun f ->
  stream >>= fun s ->
  op >>= fun o ->
  len >>= fun l ->
  if l = 0l then
    match c with
    | '\x04' -> return (Req { flags = f; stream = s; op = o; body = Empty })
    | '\x84' -> return (Res { flags = f; stream = s; op = o; body = Empty })
    | _ -> fail "invalid header"
  else
    any_char >>= fun _ ->
    body o >>= fun b ->
    match c with
    | '\x04' -> return (Req { flags = f; stream = s; op = o; body = b })
    | '\x84' -> return (Res { flags = f; stream = s; op = o; body = b })
    | _ -> fail "invalid header"
