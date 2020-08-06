open OUnit2

let suite =
  "TestSuite" >:::
  Ser_des_tests.tests @
  Protocol_tests.tests

let _ = run_test_tt_main suite
