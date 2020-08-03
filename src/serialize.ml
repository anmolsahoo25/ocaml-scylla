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
    write_char len_serializer '\x00';
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
