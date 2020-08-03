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

type body =
  | Empty
  | Map of (Angstrom.bigstring * Angstrom.bigstring) list

type packet =
  | Req of { flags : flag list; stream : int; op : opcode; body : body }
  | Res of { flags : flag list; stream : int; op : opcode; body : body }
