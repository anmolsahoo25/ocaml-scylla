type bigstring = Angstrom.bigstring

type value =
  | Null
  | Ascii of bigstring
  | Bigint of int64
  | Blob of bigstring
  | Boolean of bool
  | Counter of int64
  (* | Decimal of *)
  | Double of float
  | Float of float
  | Int of int32
  | Timestamp of int64
  (* | Uuid of *)
  | Varchar of bigstring
  | Varint of bigstring
  | Timeuuid
  | Inet
  | Date
  | Time
  | Smallint of int
  | Tinyint of char
  | List
  | Map
  | Set

type flag = Compressed | Tracing | Custom | Warning

type opcode =
  | Error
  | Startup
  | Ready
  | Authenticate
  | Options
  | Supported
  | Query
  | Result
  | Prepare
  | Execute
  | Register
  | Event
  | Batch
  | Auth_challenge
  | Auth_response
  | Auth_success

type result_body =
  | Void
  | Rows of {table_spec : (bigstring * bigstring * bigstring) array; values : value array array; paging_state : bigstring option}
  | Set_keyspace of bigstring
  | Prepared of { id : bigstring }
  | Schema_change

type consistency =
  | Any
  | One
  | Two
  | Three
  | Quorom
  | All
  | Local_quorom
  | Each_quorom
  | Serial
  | Local_serial
  | Local_one

type query_params = { consistency : consistency ; }

type body =
  | Empty
  | Map of (bigstring * bigstring) list
  | MultiMap of (bigstring * bigstring list) list
  | LongString of bigstring
  | String of bigstring
  | Result of result_body
  | Query of {query : bigstring ; params : query_params}

type packet =
  | Req of { flags : flag list; stream : int; op : opcode; body : body }
  | Res of { flags : flag list; stream : int; op : opcode; body : body }
