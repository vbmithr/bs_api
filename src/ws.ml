open Core.Std
open Core_extended.Std
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
  let r = BMEX.Ws.open_connection ~buf ~to_ws ~log:Lazy.(force log) ~auth:(key, secret) ~testnet ~topics ~md () in
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

let kaiko topics =
  let open BMEX.Ws.Kaiko in
  let buf = Bi_outbuf.create 4096 in
  let r = tickers ~log:Lazy.(force log) topics in
  Pipe.transfer r Writer.(pipe @@ Lazy.force stderr) ~f:begin fun data ->
    let json = data_to_yojson data  in
    Yojson.Safe.to_string ~buf json ^ "\n"
  end

let kaiko =
  let run _cfg loglevel _testnet _md topics =
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    don't_wait_for @@ kaiko topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Kaiko WS client" base_spec run

let bfx key secret topics =
  let open BFX.Ws in
  let evts = List.map topics ~f:begin fun ts -> match String.split ts ~on:':' with
    | [topic; symbol] ->
      BFX.Ws.Ev.create ~name:"subscribe" ~fields:["channel", `String topic; "pair", `String symbol; "prec", `String "R0"] ()
    | _ -> invalid_arg "topic"
    end
  in
  let buf = Bi_outbuf.create 4096 in
  let to_ws, to_ws_w = Pipe.create () in
  let r = open_connection ~to_ws ~buf ~auth:(key, secret) () in
  Pipe.transfer' r Writer.(pipe @@ Lazy.force stderr) ~f:begin fun q ->
    Deferred.Queue.filter_map q ~f:begin fun s ->
      Result.iter (Ev.of_yojson s) ~f:begin function
      | { name = "info" } -> don't_wait_for @@ Pipe.(transfer_id (of_list evts) to_ws_w);
      | _ -> ()
      end;
      return @@ Option.some @@ Yojson.Safe.to_string ~buf s ^ "\n"
    end
  end

let bfx =
  let run cfg loglevel _testnet _md topics =
    let key, secret = find_auth cfg "BFX" in
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    don't_wait_for @@ bfx key secret topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Bitfinex WS client" base_spec run

let plnx key secret topics =
  let open PLNX in
  let process_user_cmd () =
    let process s =
      match String.split s ~on:' ' with
      | ["balance"; currency]  ->
        Rest.balances ~key ~secret () >>| begin function
        | Error err -> error "%s" @@ Error.to_string_hum err
        | Ok resp ->
          info "found %d balances" @@ List.length resp;
          match List.Assoc.find resp currency with
        | Some b -> info "%s: %s" currency (Rest.sexp_of_balance b |> Sexplib.Sexp.to_string)
        | None -> ()
        end
      | ["balances"]  ->
        Rest.nonzero_balances ~key ~secret () >>| begin function
        | Error err -> error "%s" @@ Error.to_string_hum err
        | Ok resp -> List.iter resp ~f:begin fun (account, bs) ->
            info "%s: %s"
              (Rest.sexp_of_account account |> Sexplib.Sexp.to_string)
              Sexplib.Sexp.(List (List.map bs ~f:(fun (c, b) -> List [sexp_of_string c; sexp_of_int b])) |> to_string)
          end
        end
      | ["oos"; symbol] ->
        Rest.open_orders ~symbol ~key ~secret () >>| begin function
        | Ok resp ->
          List.iter resp ~f:(fun (s, oos) -> info "%s: %s" s (Sexplib.Std.sexp_of_list Rest.sexp_of_open_orders_resp oos |> Sexplib.Sexp.to_string))
        | Error err -> error "%s" @@ Error.to_string_hum err
        end
      | ["th"; symbol] ->
        Rest.trade_history ~symbol ~key ~secret () >>| begin function
        | Ok resp ->
          List.iter resp ~f:(fun (s, ths) -> info "%s: %s" s (Sexplib.Std.sexp_of_list Rest.sexp_of_trade_history ths |> Sexplib.Sexp.to_string))
        | Error err -> error "%s" @@ Error.to_string_hum err
        end
      | [symbol; side; price; qty] ->
        let side = match side with "b" -> Dtc.Dtc.Buy | "s" -> Sell | _ -> failwith "side" in
        let price = Int.of_string price in
        let qty = Int.of_string qty in
        Rest.order ~key ~secret ~symbol ~side ~price ~qty () >>| begin function
        | Ok resp -> info "%s" (Rest.order_response_to_yojson resp |> Yojson.Safe.to_string)
        | Error err -> error "%s" @@ Error.to_string_hum err
        end
      | h::t ->
        error "Unknown command %s" h; Deferred.unit
      | [] -> error "Empty command"; Deferred.unit
    in
    let rec loop () =
      In_thread.run Readline.input_line >>= function
      | Some msg -> process msg >>= loop
      | None -> Deferred.unit
    in loop ()
  in
  let to_ws, to_ws_w = Pipe.create () in
  let r = PLNX.Ws.open_connection ~log:(Lazy.force log) to_ws in
  let transfer_f q =
    Deferred.Queue.filter_map q ~f:begin function
    | Wamp.Welcome _ as msg ->
      PLNX.Ws.Msgpck.subscribe to_ws_w topics >>| fun _req_ids ->
      msg |> Wamp_msgpck.msg_to_msgpck |>
      Msgpck.sexp_of_t |> fun msg_str ->
      Option.some @@ Sexplib.Sexp.to_string_hum msg_str ^ "\n";
    | msg ->
      msg |> Wamp_msgpck.msg_to_msgpck |>
      Msgpck.sexp_of_t |> fun msg_str ->
      return @@ Option.some @@ Sexplib.Sexp.to_string_hum msg_str ^ "\n";
    end
  in
  Deferred.all_unit [
    process_user_cmd ();
    Pipe.transfer' r Writer.(pipe @@ Lazy.force stderr) ~f:transfer_f
  ]

let plnx =
  let run cfg loglevel _testnet _md topics =
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    let key, secret = find_auth cfg "PLNX" in
    don't_wait_for @@ plnx key secret topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Poloniex WS client" base_spec run

let plnx_trades currency =
  let open PLNX in
  let r = Rest.all_trades ~log:(Lazy.force log) currency in
  let transfer_f t = DB.sexp_of_trade t |> Sexplib.Sexp.to_string |> fun s -> s ^ "\n" in
  Pipe.transfer r Writer.(pipe @@ Lazy.force stderr) ~f:transfer_f >>= fun () ->
  Shutdown.exit 0

let plnx_trades =
  let run cfg loglevel _testnet _md topics =
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    begin match topics with
    | [] -> invalid_arg "topics"
    | currency :: _ -> don't_wait_for @@ plnx_trades currency;
    end;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Poloniex trades" base_spec run

let command =
  Command.group ~summary:"Exchanges WS client"
    [
      "bitmex", bitmex;
      "bfx", bfx;
      "plnx", plnx;
      "plnx-trades", plnx_trades;
      "kaiko", kaiko;
    ]

let () = Command.run command

