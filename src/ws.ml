open Core
open Async
open Log.Global

open Bs_devkit
module BMEX = Bitmex_api
module BFX = Bfx_api
module PLNX = Poloniex_api

let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu"
let find_auth cfg exchange =
  let cfg = Sexplib.Sexp.load_sexp_conv_exn cfg Cfg.t_of_sexp in
  let { Cfg.key; secret } =
    List.Assoc.find_exn ~equal:String.equal cfg exchange in
  key, Cstruct.of_string secret

let base_spec =
  let open Command.Spec in
  empty
  +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of cfg (default: ~/.virtu)"
  +> flag "-loglevel" (optional int) ~doc:"1-3 loglevel"
  +> flag "-testnet" no_arg ~doc:" Use testnet"
  +> flag "-md" no_arg ~doc:" Use multiplexing"
  +> flag "-rest" no_arg ~doc:" Tread stdin as input for REST commands"
  +> anon (sequence ("topic" %: string))

let bitmex key secret testnet md rest topics =
  let buf = Bi_outbuf.create 4096 in
  let process_user_cmd () =
    let process s =
      match String.split s ~on:' ' with
      | ["dtc"] ->  begin BMEX.Rest.ApiKey.dtc ~buf ~testnet ~key ~secret () >>| function
        | Error err -> error "%s" @@ Error.to_string_hum err
        | Ok entries -> info "%s" (Sexplib.Std.sexp_of_list BMEX.Rest.ApiKey.sexp_of_entry entries |> Sexplib.Sexp.to_string);
        end
      | ["position"] -> begin BMEX.Rest.Position.position ~buf ~testnet ~key ~secret () >>| function
        | Error err -> error "%s" @@ Error.to_string_hum err
        | Ok json -> info "%s" @@ Yojson.Safe.to_string ~buf json
        end
      | h::t ->
        error "Unknown command %s" h; Deferred.unit
      | [] -> error "Empty command"; Deferred.unit
    in
    let rec loop () = Reader.(read_line @@ Lazy.force stdin) >>= function
      | `Eof -> Deferred.unit
      | `Ok line -> process line >>= loop
    in
    loop ()
  in
  let to_ws = if rest then None else Option.some @@ Pipe.map Reader.(stdin |> Lazy.force |> pipe) ~f:(Yojson.Safe.from_string ~buf) in
  let r = BMEX.Ws.open_connection ~buf ?to_ws ~log:Lazy.(force log) ~auth:(key, secret) ~testnet ~topics ~md () in
  Deferred.all_unit [
    Pipe.transfer r Writer.(pipe @@ Lazy.force stderr) ~f:(fun s -> Yojson.Safe.to_string ~buf s ^ "\n");
    if rest then process_user_cmd () else Deferred.unit
  ]

let bitmex =
  let run cfg loglevel testnet md rest topics =
    let exchange = "BMEX" ^ (if testnet then "T" else "") in
    let key, secret = find_auth cfg exchange in
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    don't_wait_for @@ bitmex key secret testnet md rest topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"BitMEX WS client" base_spec run

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
  let run cfg loglevel _testnet _md _rest topics =
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
      | ["positions"] ->
        Rest.margin_positions ~key ~secret () >>| begin function
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        | Ok resp ->
          let nb_nonempty = List.fold_left resp ~init:0 ~f:begin fun i (symbol, p) ->
              match p with
              | Some p ->
                info "%s: %s" symbol (Rest.sexp_of_margin_position p |> Sexplib.Sexp.to_string);
                succ i
              | None -> i
            end
          in
          if nb_nonempty = 0 then info "No positions"
        end
      | ["margin"] ->
        Rest.margin_account_summary ~key ~secret () >>| begin function
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        | Ok resp -> info "%s" (Rest.sexp_of_margin_account_summary resp |> Sexplib.Sexp.to_string)
        end
      | ["balance"; currency]  ->
        Rest.balances ~key ~secret () >>| begin function
        | Error err -> error "%s" (Rest.Http_error.to_string err)
        | Ok resp ->
          info "found %d balances" @@ List.length resp;
          match List.Assoc.find ~equal:String.equal resp currency with
        | Some b -> info "%s: %s" currency (Rest.sexp_of_balance b |> Sexplib.Sexp.to_string)
        | None -> ()
        end
      | ["balances"]  ->
        Rest.positive_balances ~key ~secret () >>| begin function
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        | Ok resp -> List.iter resp ~f:begin fun (account, bs) ->
            info "%s: %s"
              (Rest.sexp_of_account account |> Sexplib.Sexp.to_string)
              Sexplib.Sexp.(List (List.map bs ~f:(fun (c, b) -> List [sexp_of_string c; sexp_of_int b])) |> to_string)
          end
        end
      | ["oos"] ->
        Rest.open_orders ~key ~secret () >>| begin function
        | Ok resp ->
          let nb_nonempty = List.fold_left ~init:0  resp ~f:begin fun i (s, oos) ->
              if oos <> [] then begin
                info "%s: %s" s (Sexplib.Std.sexp_of_list Rest.sexp_of_open_orders_resp oos |> Sexplib.Sexp.to_string);
                succ i
              end
              else i
            end
          in if nb_nonempty = 0 then info "No open orders"
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        end
      | ["th"; symbol] ->
        Rest.trade_history ~symbol ~key ~secret () >>| begin function
        | Ok resp ->
          List.iter resp ~f:(fun (s, ths) -> info "%s: %s" s (Sexplib.Std.sexp_of_list Rest.sexp_of_trade_history ths |> Sexplib.Sexp.to_string))
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        end
      | ["cancel"; id] ->
        let id = Int.of_string id in
        Rest.cancel ~key ~secret id >>| begin function
        | Ok () -> info "canceled order %d OK" id
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        end
      | [side; symbol; price; qty] ->
        let side = match side with "buy" -> `Buy | "sell" -> `Sell | _ -> failwith "side" in
        let price = Float.of_string price in
        let qty = Float.of_string qty in
        if margin_enabled symbol then
          Rest.margin_order ~key ~secret ~symbol ~side ~price ~qty () >>| begin function
          | Ok resp -> info "%s" (Rest.sexp_of_order_response resp |> Sexplib.Sexp.to_string)
          | Error err -> error "%s" @@ Rest.Http_error.to_string err
          end
        else
          Rest.order ~key ~secret ~symbol ~side ~price ~qty () >>| begin function
          | Ok resp -> info "%s" (Rest.sexp_of_order_response resp |> Sexplib.Sexp.to_string)
          | Error err -> error "%s" @@ Rest.Http_error.to_string err
          end
      | ["modify"; id; price] ->
        let id = Int.of_string id in
        let price = Float.of_string price in
        Rest.modify ~key ~secret ~price id >>| begin function
        | Ok resp -> info "%s" (Rest.sexp_of_order_response resp |> Sexplib.Sexp.to_string)
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        end
      | ["close"; symbol] ->
        Rest.close_position ~key ~secret symbol >>| begin function
        | Ok resp -> info "%s" (Rest.sexp_of_order_response resp |> Sexplib.Sexp.to_string)
        | Error err -> error "%s" @@ Rest.Http_error.to_string err
        end
      | h::t ->
        error "Unknown command %s" h; Deferred.unit
      | [] -> error "Empty command"; Deferred.unit
    in
    let rec loop () = Reader.(read_line @@ Lazy.force stdin) >>= function
      | `Eof -> Deferred.unit
      | `Ok line -> process line >>= loop
    in
    loop ()
  in
  let to_ws, to_ws_w = Pipe.create () in
  let r = PLNX.Ws.open_connection ~log:(Lazy.force log) to_ws in
  let transfer_f q =
    Deferred.Queue.filter_map q ~f:begin function
    | Wamp.Welcome _ as msg ->
      PLNX.Ws.Msgpck.subscribe to_ws_w topics >>| fun _req_ids ->
      msg |> Wamp_msgpck.msg_to_msgpck |> fun msgpck ->
      Some (Format.asprintf "%a@." Msgpck.pp msgpck)
    | msg ->
      msg |> Wamp_msgpck.msg_to_msgpck |> fun msgpck ->
      return (Some (Format.asprintf "%a@." Msgpck.pp msgpck))
    end
  in
  Deferred.all_unit [
    process_user_cmd ();
    Pipe.transfer' r Writer.(pipe @@ Lazy.force stderr) ~f:transfer_f
  ]

let plnx =
  let run cfg loglevel _testnet _md _rest topics =
    Option.iter loglevel ~f:(Fn.compose set_level loglevel_of_int);
    let key, secret = find_auth cfg "PLNX" in
    don't_wait_for @@ plnx key secret topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Poloniex WS client" base_spec run

let plnx_trades symbol =
  let open PLNX in
  let r = Rest.all_trades_exn
      ~log:(Lazy.force log) symbol in
  let transfer_f t = DB.sexp_of_trade t |> Sexplib.Sexp.to_string |> fun s -> s ^ "\n" in
  Pipe.transfer r Writer.(pipe @@ Lazy.force stdout) ~f:transfer_f >>= fun () ->
  Shutdown.exit 0

let plnx_trades =
  let run cfg loglevel _testnet _md _rest topics =
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
    ]

let () = Command.run command

