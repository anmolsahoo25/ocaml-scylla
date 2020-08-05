open Protocol
open Faraday

let serialize_flags serializer _flags = write_char serializer '\x00'

let serialize_op serializer op =
  let b =
    match op with
    | Error -> '\x00'
    | Startup -> '\x01'
    | Ready -> '\x02'
    | Authenticate -> '\x03'
    | Options -> '\x05'
    | Supported -> '\x06'
    | Query -> '\x07'
    | Result -> '\x08'
    | Prepare -> '\x09'
    | Execute -> '\x0a'
    | Register -> '\x0b'
    | Event -> '\x0c'
    | Batch -> '\x0d'
    | Auth_challenge -> '\x0e'
    | Auth_response -> '\x0f'
    | Auth_success -> '\x10'
  in
  write_char serializer b

let serialize_consistency serializer c =
  let b = match c with
    | Any -> 0x0
    | One -> 0x1
    | Two -> 0x2
    | Three -> 0x3
    | Quorom -> 0x4
    | All -> 0x5
    | Local_quorom -> 0x6
    | Each_quorom -> 0x7
    | Serial -> 0x8
    | Local_serial -> 0x9
    | Local_one -> 0x0a
  in
  BE.write_uint16 serializer b

let serialize_body body =
  let len_serializer = Faraday.create 0x4 in
  let body_serializer = Faraday.create 0x100 in
  match body with
  | Empty ->
    BE.write_uint32 len_serializer 0l;
    [len_serializer ; body_serializer]
  | Map l ->
    let len = ref 2 in
    BE.write_uint16 body_serializer (List.length l);
    List.iter (fun (k,v) ->
        let klen = Bigstringaf.length k in
        let vlen = Bigstringaf.length v in
        len := !len + klen + vlen + 2 + 2;
        BE.write_uint16 body_serializer klen;
        write_bigstring body_serializer k;
        BE.write_uint16 body_serializer vlen;
        write_bigstring body_serializer v
      ) l;
    BE.write_uint32 len_serializer (Int32.of_int !len);
    [len_serializer ; body_serializer]
  | LongString b ->
    let string_len = Bigstringaf.length b in
    BE.write_uint32 len_serializer (Int32.of_int (4 + string_len));
    BE.write_uint32 body_serializer (Int32.of_int string_len);
    write_bigstring body_serializer b;
    [len_serializer ; body_serializer]
  | String b ->
    let string_len = Bigstringaf.length b in
    BE.write_uint16 len_serializer (2 + string_len);
    BE.write_uint16 body_serializer string_len;
    write_bigstring body_serializer b;
    [len_serializer ; body_serializer]
  | MultiMap _ ->
    []
  | Result _ ->
    []
  | Query { query ; params }->
    let len = ref 0 in
    let query_len = Bigstringaf.length query in
    BE.write_uint32 body_serializer (Int32.of_int query_len);
    write_bigstring body_serializer query;
    len := !len + 4 + query_len;
    serialize_consistency body_serializer params.consistency;
    len := !len + 2;
    write_char body_serializer '\x00';
    len := !len + 1;
    BE.write_uint32 len_serializer (Int32.of_int !len);
    [len_serializer ; body_serializer]

let to_bytes p =
  let serializer = create 0x100 in
  let serializers =
    match p with
    | Req { flags; stream; op; body } ->
        write_char serializer '\x04';
        serialize_flags serializer flags;
        BE.write_uint16 serializer stream;
        serialize_op serializer op;
        serializer :: (serialize_body body)
    | Res { flags; stream; op; body } ->
        write_char serializer '\x84';
        serialize_flags serializer flags;
        BE.write_uint16 serializer stream;
        serialize_op serializer op;
        serializer :: (serialize_body body)
  in
  serializers
