open OUnit2
open Result
open Scylla.Protocol
open Scylla.Serialize
open Scylla.Parse

let test_header_parsing _ =
  let req_packet = Req { flags = []; stream = 0; op = Startup; body = Empty } in
  let res_packet = Res { flags = []; stream = 0; op = Result; body = Empty } in
  let bytes_req =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes req_packet))
  in
  let bytes_res =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes res_packet))
  in
  let parsed_req =
    Angstrom.parse_bigstring ~consume:Angstrom.Consume.All parse bytes_req
    |> get_ok
  in
  let parsed_res =
    Angstrom.parse_bigstring ~consume:Angstrom.Consume.All parse bytes_res
    |> get_ok
  in
  assert (parsed_req = req_packet);
  assert (parsed_res = res_packet)

let test_map_body_parsing _ =
  let k = Bigstring.of_string "\x43\x51\x4C\x5F\x56\x45\x52\x53\x49\x4F\x4E" in
  let v = Bigstring.of_string "\x33\x2E\x30\x2E\x30" in
  let res_packet =
    Res { flags = []; stream = 0; op = Options; body = Map [ (k, v) ] }
  in
  let res_bytes =
    Bigstring.concat ""
      (List.map Faraday.serialize_to_bigstring (to_bytes res_packet))
  in
  let res_parsed =
    Angstrom.parse_bigstring ~consume:Angstrom.Consume.All parse res_bytes
    |> get_ok
  in
  assert (res_parsed = res_packet)

let tests =
  [
  "test_header_parsing" >:: test_header_parsing;
  "test_map_body_parsing" >:: test_map_body_parsing;
  ]
