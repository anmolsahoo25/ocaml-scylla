open OUnit2

let dummy_test _ =
  assert ((Lwt_main.run Scylla.x) = 10)

let suite =
  "TestSuite" >::: [
    "dummy test" >:: dummy_test
  ]

let _ =
  run_test_tt_main suite
