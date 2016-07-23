open Core.Std
open Async.Std

open Bs_devkit.Core

let endpoint = Uri.of_string "https://poloniex.com/public"

type trade_raw = {
  globalTradeID: (Yojson.Safe.json [@default `Null]);
  tradeID: (Yojson.Safe.json [@default `Null]);
  date: string;
  typ: string [@key "type"];
  rate: string;
  amount: string;
  total: string;
} [@@deriving create,yojson]

type trade = {
  ts: Time_ns.t;
  side: BuyOrSell.t;
  price: int; (* in satoshis *)
  qty: int; (* in satoshis *)
} [@@deriving create, sexp, bin_io]

let trade_of_trade_raw { date; typ; rate; amount; total } =
  let date = Time_ns.of_string date in
  let typ = match typ with "buy" -> BuyOrSell.Buy | "sell" -> Sell | _ -> invalid_arg "typ_of_string" in
  let rate = Fn.compose satoshis_int_of_float_exn Float.of_string rate in
  let amount = Fn.compose satoshis_int_of_float_exn Float.of_string amount in
  create_trade date typ rate amount ()

module Ws = struct
  type book_raw = {
    rate: string;
    typ: string [@key "type"];
    amount: (string option [@default None]);
  } [@@deriving yojson]

let to_book action { rate; typ; amount } =
  let side = match typ with "bid" -> Side.Bid | "ask" -> Ask | _ -> invalid_arg "book_of_book_raw" in
  let price = Fn.compose satoshis_int_of_float_exn Float.of_string rate in
  let size = Option.map amount ~f:(Fn.compose satoshis_int_of_float_exn Float.of_string) in
  let update = OB.create_update ~id:0 ~side ~price ?size () in
  OB.create action update ()

  type t = {
    typ: string [@key "type"];
    data: Yojson.Safe.json;
  } [@@deriving yojson]

  type ticker = {
    symbol: string;
    last: float;
    ask: float;
    bid: float;
    pct_change: float;
    base_volume: float;
    quote_volume: float;
    is_frozen: bool;
    high24h: float;
    low24h: float;
  } [@@deriving show,create]

  let ticker_of_json = function
    | `List [`String symbol; `String last; `String ask; `String bid; `String pct_change;
             `String base_volume; `String quote_volume; `Int is_frozen; `String high24h;
             `String low24h
            ] ->
      let last = Float.of_string last in
      let ask = Float.of_string ask in
      let bid = Float.of_string bid in
      let pct_change = Float.of_string pct_change in
      let base_volume = Float.of_string base_volume in
      let quote_volume = Float.of_string quote_volume in
      let is_frozen = if is_frozen = 0 then false else true in
      let high24h = Float.of_string high24h in
      let low24h = Float.of_string low24h in
      create_ticker ~symbol ~last ~ask ~bid ~pct_change ~base_volume ~quote_volume ~is_frozen
        ~high24h ~low24h ()
    | #Yojson.Safe.json as json -> invalid_argf "ticker_of_json: %s" Yojson.Safe.(to_string json) ()

  let open_connection ?(buf=Bi_outbuf.create 1024) ?log ~topics () =
    let uri_str = "https://api.poloniex.com" in
    let uri = Uri.of_string uri_str in
    let host = Option.value_exn ~message:"no host in uri" Uri.(host uri) in
    let port = Option.value_exn ~message:"no port inferred from scheme"
        Uri_services.(tcp_port_of_uri uri) in
    let scheme =
      Option.value_exn ~message:"no scheme in uri" Uri.(scheme uri) in
    let write_wamp w msg = msg |> Wamp.json_of_msg |> Yojson.Safe.to_string |> Pipe.write w in
    let read_wamp msg = Yojson.Safe.from_string ~buf msg |> Wamp.msg_of_json in
    let subscribe ~topics r w =
      Deferred.List.map ~how:`Sequential topics ~f:(fun topic ->
          let request_id, subscribe_msg = Wamp.(subscribe ~topic ()) in
          write_wamp w subscribe_msg >>= fun () ->
          Pipe.read r >>| function
          | `Eof -> raise End_of_file
          | `Ok msg -> Wamp.subscribed_of_msg ~request_id @@ read_wamp msg
        )
    in
    let client_r, client_w = Pipe.create () in
    let process_ws r w =
      (* Initialize *)
      write_wamp w Wamp.(hello ~realm:"realm1" ()) >>= fun () ->
      Pipe.read r >>= function
      | `Eof -> raise End_of_file
      | `Ok msg -> begin
          let msg = read_wamp msg in
          match msg.Wamp.typ with
          | Welcome ->
            subscribe ~topics r w >>= fun _sub_ids ->
            Pipe.transfer r client_w read_wamp
          | msgtype -> failwithf "wamp: expected Welcome, got %s" Wamp.(show_msgtype msgtype) ()
        end
    in
    let tcp_fun s r w =
      Socket.(setopt s Opt.nodelay true);
      begin
        if scheme = "https" || scheme = "wss" then Conduit_async_ssl.ssl_connect r w
        else return (r, w)
      end >>= fun (r, w) ->
      let extra_headers = Cohttp.Header.init_with "Sec-Websocket-Protocol" "wamp.2.json" in
      let ws_r, ws_w =
        Websocket_async.client_ez ~extra_headers ~heartbeat:(sec 25.) uri s r w
      in
      let cleanup () = Deferred.all_unit [Reader.close r; Writer.close w] in
      maybe_info log "[WS] connecting to %s" uri_str;
      Monitor.protect ~finally:cleanup (fun () -> process_ws ws_r ws_w)
    in
    let rec loop () = begin
      Monitor.try_with_or_error ~name:"PNLX.Ws.open_connection"
        (fun () -> Tcp.(with_connection (to_host_and_port host port) tcp_fun)) >>| function
      | Ok () -> maybe_error log "[WS] connection to %s terminated" uri_str
      | Error err -> maybe_error log "[WS] connection to %s raised %s" uri_str (Error.to_string_hum err)
    end >>= fun () ->
      if Pipe.is_closed client_r then Deferred.unit
      else begin
        maybe_error log "[WS] restarting connection to %s" uri_str;
        Clock_ns.after @@ Time_ns.Span.of_int_sec 10 >>= loop
      end
    in
    don't_wait_for @@ loop ();
    client_r
end
