open Result

type t = {ic : in_channel ; oc : out_channel}

type value = Protocol.value

let show_value = Protocol.show_value

let cql_version_key = Bigstringaf.of_string "CQL_VERSION" ~off:0 ~len:11
let cql_version_val = Bigstringaf.of_string "3.3.1" ~off:0 ~len:5

let serialize_to_bytes s =
  let serializers = Serialize.(to_bytes s) in
  let buffers = List.map Faraday.serialize_to_bigstring serializers in
  let lens = List.map Bigstringaf.length buffers in
  let len = List.fold_left (+) 0 lens in
  let offs = List.fold_left (fun a b -> ((List.hd a) + b) :: a) [0] lens |> List.tl |> List.rev in
  let off_len = List.combine offs lens in
  let out = Bigstringaf.create len in
  let buffer = Bytes.create len in
  List.iter2 (fun b (o,l) -> Bigstringaf.blit b ~src_off:0 out ~dst_off:o ~len:l) buffers off_len;
  Bigstringaf.blit_to_bytes out ~src_off:0 buffer ~dst_off:0 ~len;
  buffer

let get_header ic =
  let in_buffer = Bytes.create 0x9 in
  let in_bigstring = Bigstringaf.create 0x9 in
  let read_len = input ic in_buffer 0 0x9 in
  Bigstringaf.blit_from_bytes
    in_buffer ~src_off:0 in_bigstring ~dst_off:0 ~len: read_len;
  Angstrom.parse_bigstring ~consume:Angstrom.Consume.All
    Parser.parse_header in_bigstring |> get_ok

let get_body ic len header =
  let in_buffer = Bytes.create len in
  let in_bigstring = Bigstringaf.create len in
  let read_len = input ic in_buffer 0 len in
  Bigstringaf.blit_from_bytes
    in_buffer ~src_off:0 in_bigstring ~dst_off:0 ~len:read_len;
  Angstrom.parse_bigstring ~consume:Angstrom.Consume.All
    (Parser.parse_body header) in_bigstring |> get_ok

let connect ~ip:ip ~port:port =
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string ip, port) in
  let (ic, oc) = Unix.open_connection addr in
  let startup = Protocol.(Req { flags = [] ; stream = 0; op = Startup ; body = Map [(cql_version_key, cql_version_val)]}) in
  let buffer = serialize_to_bytes startup in
  output_bytes oc buffer;
  flush oc;
  let (res, _) = get_header ic in
  let response = Protocol.((function Res {op = Ready ; _} -> true | _ -> false) res) in
  if response then Ok {ic ; oc} else Error "connection not established"


let query conn ~query:(s) =
  let {ic; oc} = conn in
  let query = Bigstringaf.of_string s ~off:0 ~len:(String.length s) in
  let query_packet = Protocol.(Req {flags = [] ; stream = 0; op = Query ; body = Query {query; params = {consistency = One}}}) in
  let buffer = serialize_to_bytes query_packet in
  output_bytes oc buffer;
  flush oc;
  let (res, len) = get_header ic in
  let response = Protocol.((function Res {op = Result; _} -> true | _ -> false) res) in
  if response then begin
    let body = get_body ic len res in
    let rows = Protocol.((function Res {body = Result (Rows {values; _}) ; _} -> values | _ -> [||]) body) in
    Ok rows
  end else
    Error "query did not succeed"
