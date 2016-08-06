open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core
module BMEX = Bitmex_api
module BFX = Bfx_api
module PLNX = Poloniex_api

let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu"
let find_auth cfg exchange =
  let cfg_json = Yojson.Safe.from_file cfg in
  let cfg = Result.ok_or_failwith @@ Cfg.of_yojson cfg_json in
  let { Cfg.key; secret } = List.Assoc.find_exn cfg exchange in
  key, Cstruct.of_string secret

let base_spec =
  let open Command.Spec in
  empty
  +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of cfg (default: ~/.virtu)"
  +> flag "-loglevel" (optional int) ~doc:"1-3 loglevel"
  +> flag "-testnet" no_arg ~doc:" Use testnet"
  +> flag "-md" no_arg ~doc:" Use multiplexing"
  +> anon (sequence ("topic" %: string))

let bitmex key secret testnet md topics =
  let buf = Bi_outbuf.create 4096 in
  let to_ws = Pipe.map Reader.(stdin |> Lazy.force |> pipe) ~f:(Yojson.Safe.from_string ~buf) in
  let r = BMEX.Ws.open_connection ~to_ws ~log:Lazy.(force log) ~auth:(key, secret) ~testnet ~topics ~md () in
  Pipe.transfer r Writer.(pipe @@ Lazy.force stderr) ~f:(fun s -> Yojson.Safe.to_string ~buf s ^ "\n")

let bitmex =
  let run cfg loglevel testnet md topics =
    let exchange = "BMEX" ^ (if testnet then "T" else "") in
    let key, secret = find_auth cfg exchange in
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    don't_wait_for @@ bitmex key secret testnet md topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"BitMEX WS client" base_spec run

let bfx key secret topics =
  let evts = List.map topics ~f:(fun ts ->
      match String.split ts ~on:':' with
      | [topic; symbol] ->
        BFX.Ws.Ev.create ~name:"subscribe" ~fields:["channel", `String topic; "pair", `String symbol; "prec", `String "R0"] ()
      | _ -> invalid_arg "topic"
    ) in
  let r = BFX.Ws.open_connection ~auth:(key, secret) ~evts () in
  Pipe.transfer r Writer.(pipe @@ Lazy.force stderr) ~f:(fun s -> s ^ "\n")

let bfx =
  let run cfg loglevel _testnet _md topics =
    let key, secret = find_auth cfg "BFX" in
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    don't_wait_for @@ bfx key secret topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Bitfinex WS client" base_spec run

let plnx topics =
  let topics = List.map topics ~f:Uri.of_string in
  let r = PLNX.Ws.open_connection ~topics () in
  let buf = Bi_outbuf.create 4096 in
  Pipe.transfer r Writer.(pipe @@ Lazy.force stderr)
    ~f:(fun msg -> msg |> Wamp.msg_to_yojson |> Yojson.Safe.to_string ~buf |> fun msg_str -> msg_str ^ "\n")

let plnx =
  let run cfg loglevel _testnet _md topics =
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    don't_wait_for @@ plnx topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Poloniex WS client" base_spec run

let command =
  Command.group ~summary:"Exchanges WS client"
    [
      "bitmex", bitmex;
      "bfx", bfx;
      "plnx", plnx;
    ]

let () = Command.run command

