type bigstring = Angstrom.bigstring

type req

type res

type 'a op =
  | Startup : req op
  | Auth_response : req op
  | Options : req op
  | Query : req op
  | Prepare : req op
  | Execute : req op
  | Batch : req op
  | Register : req op
  | Error : res op
  | Ready : res op
  | Authenticate : res op
  | Supported : res op
  | Result : res op
  | Event : res op
  | Auth_challenge : res op
  | Auth_success : res op

type flag = Compressed | Tracing | Custom | Warning

type empty

type map

type multimap

type 'a body =
  | Empty : empty body
  | Map : (bigstring * bigstring) list -> map body
  | MultiMap : (bigstring * (bigstring list)) list -> multimap body

type ('a, 'b) packet =
  | Req : {flags : flag list; stream: int; op : req op ; body: 'b body } -> (req, 'b) packet
