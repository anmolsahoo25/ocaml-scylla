open OUnit2
open Result
open Scylla.Protocol
open Scylla.Serialize
open Scylla.Parse

let cql_version_key =
  Bigstring.of_string "\x43\x51\x4C\x5F\x56\x45\x52\x53\x49\x4F\x4E"

let rec extract_cql_version = function
  | [] -> error "not present"
  | hd :: tl -> (
      match hd with
      | k, v ->
          if k = cql_version_key then ok (List.hd v) else extract_cql_version tl
      )

let test_scylla_connection_options _ =
  let options = Req { flags = []; stream = 0; op = Options; body = Empty } in
  let serialized =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes options))
  in
  let output_packet = Bytes.of_string (Bigstring.to_string serialized) in
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string "172.17.0.2", 9042) in
  let c = Unix.open_connection addr in
  let ic = fst c in
  let oc = snd c in
  output_bytes oc output_packet;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let parsed_packet =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res _ -> true | _ -> false) parsed_packet = true);
  assert (
    (function Res { op; _ } -> op = Supported | _ -> false) parsed_packet
    = true );
  let body =
    (function Res { body = MultiMap l; _ } -> l | _ -> []) parsed_packet
  in
  let cql_version = Bigstring.to_string (extract_cql_version body |> get_ok) in
  assert (cql_version = "3.3.1")

let test_scylla_startup _ =
  let k = Bigstring.of_string "\x43\x51\x4C\x5F\x56\x45\x52\x53\x49\x4F\x4E" in
  let v = Bigstring.of_string "\x33\x2E\x33\x2E\x31" in
  let req_packet =
    Req { flags = []; stream = 0; op = Startup ; body = Map [ (k, v) ] }
  in
  let req_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes req_packet))
  in
  let output_packet = Bytes.of_string (Bigstring.to_string req_bytes) in
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string "172.17.0.2", 9042) in
  let c = Unix.open_connection addr in
  let ic = fst c in
  let oc = snd c in
  output_bytes oc output_packet;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let parsed_packet =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res _ -> true | _ -> false) parsed_packet = true);
  assert (
    (function Res { op; _ } -> op = Ready | _ -> false) parsed_packet
    = true );
  let body =
    (function Res { body = Empty ; _ } -> true | _ -> false) parsed_packet
  in
  assert (body = true)

let test_scylla_prepare _ =
  let k = Bigstring.of_string "\x43\x51\x4C\x5F\x56\x45\x52\x53\x49\x4F\x4E" in
  let v = Bigstring.of_string "\x33\x2E\x33\x2E\x31" in
  let q = Bigstring.of_string "\x73\x65\x6C\x65\x63\x74\x20\x2A\x20\x66\x72\x6F\x6D\x20\x6B\x65\x79\x73\x70\x61\x63\x65\x31\x2E\x70\x65\x72\x73\x6F\x6E\x20\x77\x68\x65\x72\x65\x20\x69\x64\x20\x3D\x20\x3A\x6E\x61\x6D\x65" in
  let start_packet =
    Req { flags = []; stream = 0; op = Startup ; body = Map [ (k, v) ] }
  in
  let prepare_packet =
    Req {flags = [] ; stream = 0; op = Prepare ; body = LongString q } in
  let start_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes start_packet))
  in
  let prepare_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes prepare_packet))
  in
  let start = (Bigstring.to_bytes start_bytes) in
  let prepare = (Bigstring.to_bytes prepare_bytes) in
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string "172.17.0.2", 9042) in
  let c = Unix.open_connection addr in
  let ic = fst c in
  let oc = snd c in
  output_bytes oc start;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let start_res =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res {op = Ready;_} -> true | _ -> false) start_res = true);
  output_bytes oc prepare;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let prepare_res =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res {op = Result;_} -> true | _ -> false) prepare_res = true);
  assert ((function Res {body = Result (Prepared _) ; _} -> true | _ -> false) prepare_res = true)

let test_scylla_query _ =
  let k = Bigstring.of_string "\x43\x51\x4C\x5F\x56\x45\x52\x53\x49\x4F\x4E" in
  let v = Bigstring.of_string "\x33\x2E\x33\x2E\x31" in
  let q = Bigstring.of_string "\x73\x65\x6C\x65\x63\x74\x20\x2A\x20\x66\x72\x6F\x6D\x20\x6B\x65\x79\x73\x70\x61\x63\x65\x31\x2E\x70\x65\x72\x73\x6F\x6E" in
  let start_packet =
    Req { flags = []; stream = 0; op = Startup ; body = Map [ (k, v) ] }
  in
  let query_packet =
    Req {flags = [] ; stream = 0; op = Query ; body = Query {query = q; 
    values = [||];
    params = {consistency = One ; paging_state = None ; page_size = None ; serial_consistency = None}}} in
  let start_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes start_packet))
  in
  let query_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes query_packet))
  in
  let start = (Bigstring.to_bytes start_bytes) in
  let query = (Bigstring.to_bytes query_bytes) in
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string "172.17.0.2", 9042) in
  let c = Unix.open_connection addr in
  let ic = fst c in
  let oc = snd c in
  output_bytes oc start;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let start_res =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res {op = Ready;_} -> true | _ -> false) start_res = true);
  output_bytes oc query;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let query_res =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res {op = Result ;_} -> true | _ -> false) query_res = true);
  assert ((function Res {body = Result (Rows _) ; _} -> true | _ -> false) query_res = true);
  assert ((function Res {body = Result (Rows {paging_state = None; _}); _} -> true | _ -> false) query_res = true);
  let table_spec = (function Res {body = Result (Rows {table_spec; _}); _} -> table_spec | _ -> [||]) query_res in
  Array.iter (fun (k,t,_) ->
      assert (Bigstring.to_string k = "keyspace1");
      assert (Bigstring.to_string t = "person")) table_spec;
  assert (Bigstring.to_string ((fun (_,_,n) -> n) table_spec.(0)) = "id");
  assert (Bigstring.to_string ((fun (_,_,n) -> n) table_spec.(1)) = "name");
  let values = (function Res {body = Result (Rows {values; _}); _} -> values | _ -> [||]) query_res in
  assert (values.(1).(0) = Varchar (Bigstring.of_string "1"));
  assert (values.(1).(1) = Varchar (Bigstring.of_string "person1"));
  assert (values.(0).(0) = Varchar (Bigstring.of_string "2"));
  assert (values.(0).(1) = Varchar (Bigstring.of_string "person2"))

let test_scylla_query_values _ =
  let k = Bigstring.of_string "\x43\x51\x4C\x5F\x56\x45\x52\x53\x49\x4F\x4E" in
  let v = Bigstring.of_string "\x33\x2E\x33\x2E\x31" in
  let q = Bigstring.of_string "\x73\x65\x6C\x65\x63\x74\x20\x2A\x20\x66\x72\x6F\x6D\x20\x6B\x65\x79\x73\x70\x61\x63\x65\x31\x2E\x70\x65\x72\x73\x6F\x6E\x20\x77\x68\x65\x72\x65\x20\x69\x64\x20\x3D\x20\x3F" in
  let start_packet =
    Req { flags = []; stream = 0; op = Startup ; body = Map [ (k, v) ] }
  in
  let query_packet =
    Req {flags = [] ; stream = 0; op = Query ; body = Query {query = q; 
    values = [|Varchar (Bigstring.of_string "\x31")|];
    params = {consistency = One ; paging_state = None ; page_size = None ; serial_consistency = None}}} in
  let start_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes start_packet))
  in
  let query_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes query_packet))
  in
  let start = (Bigstring.to_bytes start_bytes) in
  let query = (Bigstring.to_bytes query_bytes) in
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string "172.17.0.2", 9042) in
  let c = Unix.open_connection addr in
  let ic = fst c in
  let oc = snd c in
  output_bytes oc start;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let start_res =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res {op = Ready;_} -> true | _ -> false) start_res = true);
  output_bytes oc query;
  flush oc;
  let b = Bytes.create 0x500 in
  let _l = input ic b 0 0x500 in
  let query_res =
    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix parse
      (Bytes.to_string b)
    |> get_ok
  in
  assert ((function Res {op = Result ;_} -> true | _ -> false) query_res = true);
  assert ((function Res {body = Result (Rows _) ; _} -> true | _ -> false) query_res = true);
  assert ((function Res {body = Result (Rows {paging_state = None; _}); _} -> true | _ -> false) query_res = true);
  let table_spec = (function Res {body = Result (Rows {table_spec; _}); _} -> table_spec | _ -> [||]) query_res in
  Array.iter (fun (k,t,_) ->
      assert (Bigstring.to_string k = "keyspace1");
      assert (Bigstring.to_string t = "person")) table_spec;
  assert (Bigstring.to_string ((fun (_,_,n) -> n) table_spec.(0)) = "id");
  assert (Bigstring.to_string ((fun (_,_,n) -> n) table_spec.(1)) = "name");
  let values = (function Res {body = Result (Rows {values; _}); _} -> values | _ -> [||]) query_res in
  assert (values.(0).(0) = Varchar (Bigstring.of_string "1"));
  assert (values.(0).(1) = Varchar (Bigstring.of_string "person1"))

let tests =
  [
    "test_scylla_connection_options" >:: test_scylla_connection_options;
    "test_scylla_startup" >:: test_scylla_startup;
    "test_scylla_prepare" >:: test_scylla_prepare;
    "test_scylla_query" >:: test_scylla_query;
    "test_scylla_query_values" >:: test_scylla_query_values
  ]
