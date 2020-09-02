open Result

type conn = { ic : in_channel; oc : out_channel }

type bigstring = Bigstringaf.t

type value = Protocol.value

type rows = {
  table_spec : (bigstring * bigstring * bigstring) array;
  values : value array array;
}

let show_value = Protocol.show_value

let cql_version_key = Bigstringaf.of_string "CQL_VERSION" ~off:0 ~len:11

let cql_version_val = Bigstringaf.of_string "3.3.1" ~off:0 ~len:5

let serialize_to_bytes s =
  let serializers = Serialize.(to_bytes s) in
  let buffers = List.map Faraday.serialize_to_bigstring serializers in
  let lens = List.map Bigstringaf.length buffers in
  let len = List.fold_left ( + ) 0 lens in
  let offs =
    List.fold_left (fun a b -> (List.hd a + b) :: a) [ 0 ] lens
    |> List.tl |> List.rev
  in
  let off_len = List.combine offs lens in
  let out = Bigstringaf.create len in
  let buffer = Bytes.create len in
  List.iter2
    (fun b (o, l) -> Bigstringaf.blit b ~src_off:0 out ~dst_off:o ~len:l)
    buffers off_len;
  Bigstringaf.blit_to_bytes out ~src_off:0 buffer ~dst_off:0 ~len;
  buffer

let get_header ic =
  let in_buffer = Bytes.create 0x9 in
  let in_bigstring = Bigstringaf.create 0x9 in
  let read_len = input ic in_buffer 0 0x9 in
  Printf.printf "%S\n" (Bytes.to_string in_buffer);
  Bigstringaf.blit_from_bytes in_buffer ~src_off:0 in_bigstring ~dst_off:0
    ~len:read_len;
  match (Angstrom.parse_bigstring ~consume:Angstrom.Consume.All Parse.parse_header
           in_bigstring) with
  | Ok v -> v
  | Error e -> print_endline e ; raise Not_found


let get_body ic len header =
  let in_buffer = Bytes.create len in
  let in_bigstring = Bigstringaf.create len in
  let read_len = input ic in_buffer 0 len in
  Bigstringaf.blit_from_bytes in_buffer ~src_off:0 in_bigstring ~dst_off:0
    ~len:read_len;
  let p = Angstrom.parse_bigstring ~consume:Angstrom.Consume.All
    (Parse.parse_body header) in_bigstring in
  try
    p |> get_ok
    with _ -> (print_endline "get_body: " ^ (p |> get_error)) ; Res {flags = [] ; stream = 0; op = Startup ; body = Empty}

let connect ~ip ~port =
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string ip, port) in
  let ic, oc = Unix.open_connection addr in
  let startup =
    Protocol.(
      Req
        {
          flags = [];
          stream = 0;
          op = Startup;
          body = Map [ (cql_version_key, cql_version_val) ];
        })
  in
  let buffer = serialize_to_bytes startup in
  output_bytes oc buffer;
  flush oc;
  let res, _ = get_header ic in
  let response =
    Protocol.((function Res { op = Ready; _ } -> true | _ -> false) res)
  in
  if response then Ok { ic; oc } else Error "connection not established"

let default_query_params =
  Protocol.
    {
      consistency = One;
      page_size = None;
      paging_state = None;
      serial_consistency = None;
    }

let create_query_packet query values =
  Protocol.(
    Req
      {
        flags = [];
        stream = 0;
        op = Query;
        body = Query { query; values; params = default_query_params };
      })

let query conn ~query:s ?(values = [||]) () =
  let { ic; oc } = conn in
  let query = Bigstringaf.of_string s ~off:0 ~len:(String.length s) in
  let query_packet = create_query_packet query values in
  let buffer = serialize_to_bytes query_packet in
  output_bytes oc buffer;
  flush oc;
  let res, len = get_header ic in
  let response =
    Protocol.((function Res { op = Result; _ } -> true | _ -> false) res)
  in
  if response then
    let body = get_body ic len res in
    let table_spec, values =
      Protocol.(
        (function
          | Res { body = Result (Rows { values; table_spec; _ }); _ } ->
              (table_spec, values)
          | _ -> ([||], [||]))
          body)
    in
    Ok { table_spec; values }
  else
    let _ = get_body ic len res in
    Error "query did not succeed"

module Protocol = Protocol
module Parse = Parse
module Serialize = Serialize
