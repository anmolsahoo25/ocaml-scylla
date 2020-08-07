open Lwt.Syntax
open Option
open Result

type node = Node of int * node | Empty

let add l v = l := Node (v, !l)

let get_next l =
  match !l with
  | Empty -> None
  | Node (v, t) ->
      l := t;
      Some v

let init l n =
  List.iter (fun x -> add l x) (List.init n (fun i -> i));
  l

type conn = {
  freelist : node ref;
  slots : (Scylla.rows Lwt.u option) array;
  ic : Lwt_io.input_channel;
  oc : Lwt_io.output_channel;
  fd : Lwt_unix.file_descr
}

let response_worker ic conn =
  let open Scylla in
  let open Scylla.Protocol in
  let wakeup_promise res =
    match res with
    | Res {stream; op = Result; body = Result (Rows {table_spec ; values; _}); _ } ->
      Lwt.wakeup (conn.slots.(stream) |> get) {table_spec ; values };
      add conn.freelist stream;
      Lwt.return ()
    | _ -> failwith "not implemented"
  in
  Angstrom_lwt_unix.parse_many
    Scylla.Parse.parse
    wakeup_promise
    ic

let cql_version_key = Bigstringaf.of_string "CQL_VERSION" ~off:0 ~len:11

let cql_version_val = Bigstringaf.of_string "3.3.1" ~off:0 ~len:5

let start_packet =
  let open Scylla.Protocol in
    Req {
      flags = [];
      stream = 0;
      op = Startup;
      body = Map [(cql_version_key, cql_version_val)]
    }

let send_iovecs l fd =
  let open Lwt_unix in
  let open Faraday in
  let iov = IO_vectors.create () in
  List.iter (fun x -> IO_vectors.append_bigarray iov x.buffer x.off x.len) l;
  let* _ = writev fd iov in
  Lwt.return (`Closed)

let send_packet packet fd =
  let serializers = Scylla.Serialize.to_bytes packet in
  let send_serializer s =
    Faraday_lwt_unix.serialize s
      ~yield: (fun _ -> Lwt.return ())
      ~writev:(fun l -> send_iovecs l fd) in
  Lwt_list.iter_s send_serializer serializers

let connect ~ip ~port =
  let fd = Lwt_unix.(socket PF_INET SOCK_STREAM 0) in
  let inet = Unix.(inet_addr_of_string ip) in
  let addr = Lwt_unix.(ADDR_INET (inet, port)) in
  let ic = Lwt_io.(of_fd ~mode:input fd) in
  let oc = Lwt_io.(of_fd ~mode:output fd) in
  let freelist = init (ref Empty) 1024 in
  let slots = Array.make 1024 None in
  let* _ = Lwt_unix.connect fd addr in
  let* _ = send_packet start_packet fd in
  let* (_, res) = Angstrom_lwt_unix.parse Scylla.Parse.parse_header ic in
  let open Scylla.Protocol in
  let ready = (function Res {op = Ready; _} -> true | _ -> false) (fst (res |> get_ok)) in
  if ready then begin
  let conn = { freelist; slots; ic; oc ; fd} in
  let _ = response_worker ic conn in
  Lwt.return conn
  end else Lwt.fail_with "not ready"

let default_query_params =
  Scylla.Protocol.
    {
      consistency = One;
      page_size = None;
      paging_state = None;
      serial_consistency = None;
    }

let create_query_packet query values stream =
  Scylla.Protocol.(
    Req
      {
        flags = [];
        stream = stream;
        op = Query;
        body = Query { query ; values ; params = default_query_params };
      })

let query conn ~query:s ?values:(values = [||]) () =
  let stream = get_next conn.freelist |> get in
  let query = Bigstringaf.of_string s ~off:0 ~len:(String.length s) in
  let packet = create_query_packet query values stream in
  let (promise, resolver) = Lwt.wait () in
  conn.slots.(stream) <- Some resolver;
  let* _ = send_packet packet conn.fd in
  let* rows = promise in
  Lwt.return rows
