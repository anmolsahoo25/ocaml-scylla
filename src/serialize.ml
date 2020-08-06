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
  let b =
    match c with
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

let serialize_value serializer len = function
  | Varchar s ->
    let l = Bigstringaf.length s in
    BE.write_uint32 serializer (Int32.of_int l);
    write_bigstring serializer s;
    len := !len + 4 + l
  | _ -> failwith "not implemented"

let serialize_values serializer params values len =
  let values_len = Array.length values in
  let has_values = values_len > 0 in
  let values_flag = if has_values then 0b1 else 0b0 in
  let skip_metadata_flag = 0b00 in
  let page_size_flag =
    match params.page_size with None -> 0b000 | _ -> 0b100
  in
  let paging_state_flag =
    match params.paging_state with None -> 0b0000 | _ -> 0b1000
  in
  let sc_flag =
    match params.serial_consistency with None -> 0b00000 | _ -> 0b10000
  in
  let default_timestamp_flag = 0b000000 in
  let with_names_flag = 0b0000000 in
  let flag =
    values_flag lor skip_metadata_flag lor page_size_flag lor paging_state_flag
    lor sc_flag lor default_timestamp_flag lor with_names_flag
  in
  write_char serializer (char_of_int flag);
  len := !len + 1;
  if has_values then begin
    BE.write_uint16 serializer values_len;
    len := !len + 2;
    Array.iter (serialize_value serializer len) values;
  end else ()

let serialize_body body =
  let len_serializer = Faraday.create 0x4 in
  let body_serializer = Faraday.create 0x100 in
  match body with
  | Empty ->
      BE.write_uint32 len_serializer 0l;
      [ len_serializer; body_serializer ]
  | Map l ->
      let len = ref 2 in
      BE.write_uint16 body_serializer (List.length l);
      List.iter
        (fun (k, v) ->
          let klen = Bigstringaf.length k in
          let vlen = Bigstringaf.length v in
          len := !len + klen + vlen + 2 + 2;
          BE.write_uint16 body_serializer klen;
          write_bigstring body_serializer k;
          BE.write_uint16 body_serializer vlen;
          write_bigstring body_serializer v)
        l;
      BE.write_uint32 len_serializer (Int32.of_int !len);
      [ len_serializer; body_serializer ]
  | LongString b ->
      let string_len = Bigstringaf.length b in
      BE.write_uint32 len_serializer (Int32.of_int (4 + string_len));
      BE.write_uint32 body_serializer (Int32.of_int string_len);
      write_bigstring body_serializer b;
      [ len_serializer; body_serializer ]
  | String b ->
      let string_len = Bigstringaf.length b in
      BE.write_uint32 len_serializer (2 + string_len |> Int32.of_int);
      BE.write_uint16 body_serializer string_len;
      write_bigstring body_serializer b;
      [ len_serializer; body_serializer ]
  | MultiMap _ -> failwith "not implemented"
  | Result _ -> failwith "not implemented"
  | Query { query; values; params } ->
      let len = ref 0 in
      let query_len = Bigstringaf.length query in
      BE.write_uint32 body_serializer (Int32.of_int query_len);
      write_bigstring body_serializer query;
      len := !len + 4 + query_len;
      serialize_consistency body_serializer params.consistency;
      len := !len + 2;
      serialize_values body_serializer params values len;
      BE.write_uint32 len_serializer (Int32.of_int !len);
      [ len_serializer; body_serializer ]

let to_bytes p =
  let serializer = create 0x100 in
  let serializers =
    match p with
    | Req { flags; stream; op; body } ->
        write_char serializer '\x04';
        serialize_flags serializer flags;
        BE.write_uint16 serializer stream;
        serialize_op serializer op;
        serializer :: serialize_body body
    | Res { flags; stream; op; body } ->
        write_char serializer '\x84';
        serialize_flags serializer flags;
        BE.write_uint16 serializer stream;
        serialize_op serializer op;
        serializer :: serialize_body body
  in
  serializers
