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

let parse_bytes =
  BE.any_int32 >>= fun n -> take_bigstring (Int32.to_int n)

let string =
  BE.any_uint16 >>= fun n -> take_bigstring n

let string_list =
  BE.any_uint16 >>= fun n -> count n string

let string_pair =
  string >>= fun s1 ->
  string >>= fun s2 ->
  return (s1,s2)

let string_pairlist =
  string >>= fun s1 ->
  string_list >>= fun s2 ->
  return (s1,s2)

let string_map =
  BE.any_uint16 >>= fun n -> count n string_pair

let string_multimap =
  BE.any_uint16 >>= fun n -> count n string_pairlist

let process_flags flags =
  let open Int32 in
  let global_table_spec = (logand flags 0b1l) = 0b1l in
  let has_more_pages = (logand flags 0b10l) = 0b10l in
  let no_metadata = (logand flags 0b100l) = 0b100l in
  (global_table_spec, has_more_pages, no_metadata)

let rec col_type_option = function
    | 0x0
    | 0x1
    | 0x2
    | 0x3
    | 0x4
    | 0x5
    | 0x6
    | 0x7
    | 0x8
    | 0x9
    | 0xb
    | 0xc
    | 0xd
    | 0xe
    | 0xf
    | 0x10
    | 0x11
    | 0x12
    | 0x13
    | 0x14 as p -> return p
    | 0x22 -> BE.any_uint16 >>= fun o -> col_type_option o
    | _ -> fail "not implemented"

let col_spec global_table_spec =
  let table_spec = if global_table_spec then (return (Bigstringaf.empty, Bigstringaf.empty)) else string_pair in
  table_spec >>= fun (k,t) ->
  string >>= fun n ->
  BE.any_uint16 >>= fun o ->
  col_type_option o >>= fun s ->
  return (k,t,n,(o,s))

let process_col_spec global_table_spec num_cols =
  let table_spec =
    if (global_table_spec) then string_pair else (return (Bigstringaf.empty, Bigstringaf.empty)) in
  table_spec >>= fun (k,t) ->
  (count num_cols (col_spec global_table_spec)) >>= fun l ->
  let l = Array.of_list l in
  if global_table_spec then
    return (Array.map (fun (_,_,n,o) -> (k,t,n,o)) l)
  else
    return l

let rec value t =
  let (o,t) = t in
  BE.any_int32 >>= fun n ->
  let n = Int32.to_int n in
  if (n == -1) then
    return Null
  else
    match o with
    | 0x0 -> fail "not implemented"
    | 0x1 -> take_bigstring n >>= fun b -> return (Ascii b)
    | 0x2 -> BE.any_int64 >>= fun b -> return (Bigint b)
    | 0x3 -> take_bigstring n >>= fun b -> return (Blob b)
    | 0x4 -> any_char >>= fun c -> return (if (c = '\x00') then Boolean false else Boolean true)
    | 0x5 -> BE.any_int64 >>= fun n -> return (Counter n)
    | 0x9 -> BE.any_int32 >>= fun n -> return (Int n)
    | 0xd -> take_bigstring n >>= fun s -> return (Varchar s)
    | 0x22 -> BE.any_int32 >>= fun n -> count (Int32.to_int n) (value (t, o)) >>= fun l -> return (Set l)
    | _ -> fail "not implemented"

let row t =
  let ps = Array.map value t in
 (list (Array.to_list ps)) >>= fun l -> return (Array.of_list l)

let process_result_body () =
  BE.any_int32 >>= fun flags ->
  let (global_table_spec, has_more_pages, no_metadata) = process_flags flags in
  BE.any_int32 >>= fun num_cols ->
  let paging = if has_more_pages then parse_bytes else (return Bigstringaf.empty) in
  paging >>= fun paging_state_val ->
  let paging_state = if has_more_pages then Some paging_state_val else None in
  if no_metadata then
    return (Result (Rows {table_spec = [||] ; values = [||]; paging_state}))
  else begin
    process_col_spec global_table_spec (Int32.to_int num_cols) >>= fun l ->
    BE.any_int32 >>= fun num_rows ->
    let types = Array.map (fun (_,_,_,(o,s)) -> (o,s)) l in
    count (Int32.to_int num_rows) (row types) >>= fun values ->
    let table_spec = Array.map (fun (k,t,n,_) -> (k,t,n)) l in
    let values = Array.of_list values in
    return (Result (Rows {table_spec ; values; paging_state}))
  end

let result_body k l = match k with
  | 1l -> return (Result Void)
  | 2l -> process_result_body ()
  | 3l -> string >>= fun s -> return (Result (Set_keyspace s))
  | 4l ->
    BE.any_uint16 >>= fun n ->
    take_bigstring n >>= fun s ->
    return (Result (Prepared {id = s}))
  | 5l -> take_bigstring (l - 4) >>= fun _ -> return (Result Schema_change)
  | _ -> fail "no such result"

let body op l =
  match op with
  | Options -> string_map >>= fun l -> return (Map l)
  | Supported -> string_multimap >>= fun l -> return (MultiMap l)
  | Result -> BE.any_int32 >>= fun k -> result_body k l
  | Error -> BE.any_int32 >>= fun n -> string >>= fun s ->
    print_endline ("error code " ^ (string_of_int (Int32.to_int n)));
    print_endline ("error msg " ^ (Bigstringaf.to_string s));
    return Empty
  | _ -> return Empty

let parse_header =
  header >>= fun c ->
  flags >>= fun f ->
  stream >>= fun s ->
  op >>= fun o ->
  len >>= fun l ->
  match c with
  | '\x04' -> return (Req {flags = f; stream = s; op = o; body = Empty}, Int32.to_int l)
  | '\x84' -> return (Res {flags = f; stream = s; op = o; body = Empty}, Int32.to_int l)
  | _ -> fail "invalid header"

let get_op = function Req {op; _} -> op | Res {op;_} -> op

let update_body body = function Req r -> Req {r with body} | Res r -> Res {r with body}

let parse_body r l =
  body (get_op r) l >>= fun b ->
  return (update_body b r)

let parse =
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
    body o (Int32.to_int l) >>= fun b ->
    match c with
    | '\x04' -> return (Req { flags = f; stream = s; op = o; body = b })
    | '\x84' -> return (Res { flags = f; stream = s; op = o; body = b })
    | _ -> fail "invalid header"
