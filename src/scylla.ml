open Result

type t = {ic : in_channel ; oc : out_channel}

type value = Protocol.value

let serialize_to_bytes s =
  let buffers = List.map Faraday.serialize_to_bigstring s in
  let lens = List.map Bigstringaf.length buffers in
  let len = List.fold_left (+) 0 lens in
  let offs = List.fold_left (fun a b -> ((List.hd a) + b) :: a) [0] lens |> List.tl |> List.rev in
  let off_len = List.combine offs lens in
  let out = Bigstringaf.create len in
  let buffer = Bytes.create len in
  List.iter2 (fun b (o,l) -> Bigstringaf.blit b ~src_off:0 out ~dst_off:o ~len:l) buffers off_len;
  Bigstringaf.blit_to_bytes out ~src_off:0 buffer ~dst_off:0 ~len;
  buffer

let connect ~ip:ip ~port:port =
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string ip, port) in
  let (ic, oc) = Unix.open_connection addr in
  let startup = Protocol.(Req { flags = [] ; stream = 0; op = Startup ; body = Empty }) in
  let serializers = Serialize.(to_bytes startup) in
  let buffer = serialize_to_bytes serializers in
  output_bytes oc buffer;
  flush oc;
  let in_buffer = Bytes.create 0x9 in
  let in_bigstring = Bigstringaf.create 0x9 in
  let read_len = input ic in_buffer 0 0x9 in
  Bigstringaf.blit_from_bytes in_buffer ~src_off:0 in_bigstring ~dst_off: 0 ~len:read_len;
  let res = Angstrom.parse_bigstring ~consume:Angstrom.Consume.All Parser.parser in_bigstring |> get_ok in
  let response = Protocol.((function Res {op = Ready ; _} -> true | _ -> false) res) in
  if response then Ok {ic ; oc} else Error "connection not established"

let query _conn ~query:(_query) =
  [||]
