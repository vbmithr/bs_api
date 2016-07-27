open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core
module BMEX = Bitmex_api

let bitmex key secret testnet topics =
  let r = BMEX.Ws.open_connection ~auth:(key, secret) ~testnet ~topics () in
  Pipe.transfer r Writer.(pipe @@ Lazy.force stderr) ~f:(fun s -> s ^ "\n")

let bitmex =
  let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu" in
  let spec =
    let open Command.Spec in
    empty
    +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of cfg (default: ~/.virtu)"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-loglevel" (optional int) ~doc:"1-3 loglevel"
    +> anon (sequence ("topic" %: string))
  in
  let run cfg testnet loglevel topics =
    let exchange = "BMEX" ^ (if testnet then "T" else "") in
    let cfg_json = Yojson.Safe.from_file cfg in
    let cfg = Result.ok_or_failwith @@ Cfg.of_yojson cfg_json in
    let { Cfg.key; secret } = List.Assoc.find_exn cfg exchange in
    let secret = Cstruct.of_string secret in
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    don't_wait_for @@ bitmex key secret testnet topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"BitMEX WS client" spec run

let command =
  Command.group ~summary:"Exchanges WS client"
    [
      "bitmex", bitmex;
    ]

let () = Command.run command

