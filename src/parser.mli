val parse_header : (Protocol.packet * int) Angstrom.t

val parse_body : Protocol.packet -> Protocol.packet Angstrom.t

val parse : Protocol.packet Angstrom.t
