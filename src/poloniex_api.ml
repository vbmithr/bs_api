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

let trade_of_trade_raw { date; typ; rate; amount; total } =
  let date = Time_ns.of_string date in
  let typ = match typ with "buy" -> BuyOrSell.Buy | "sell" -> Sell | _ -> invalid_arg "typ_of_string" in
  let rate = Fn.compose satoshis_int_of_float_exn Float.of_string rate in
  let amount = Fn.compose satoshis_int_of_float_exn Float.of_string amount in
  DB.create_trade date typ rate amount ()

module Rest = struct
  open Cohttp_async

  let bids_asks_of_yojson side records =
    List.map records ~f:(function
      | `List [`String price; `Int qty] -> DB.create_book_entry side (Fn.compose satoshis_int_of_float_exn Float.of_string price) (Fn.compose satoshis_int_of_float_exn Float.of_int qty) ()
      | `List [`String price; `Float qty] -> DB.create_book_entry side (Fn.compose satoshis_int_of_float_exn Float.of_string price) (satoshis_int_of_float_exn qty) ()
      | #Yojson.Safe.json -> invalid_arg "books_of_yojson (record)")

  type book_raw = {
    asks: Yojson.Safe.json list;
    bids: Yojson.Safe.json list;
    isFrozen: string;
    seq: int;
  } [@@deriving yojson]

  type books = {
    asks: DB.book_entry list;
    bids: DB.book_entry list;
    isFrozen: bool;
    seq: int;
  } [@@deriving create]

  let orderbook ?log ?(depth=100) symbol =
    let url = Uri.with_query' endpoint
        ["command", "returnOrderBook"; "currencyPair", symbol; "depth", Int.to_string depth]
    in
    Client.get url >>= fun (resp, body) ->
    Body.to_string body >>| fun body_str ->
    maybe_debug log "%s" body_str;
    match Yojson.Safe.from_string body_str with
    | `Assoc ["error", `String msg] -> failwith msg
    | #Yojson.Safe.json as json ->
      book_raw_of_yojson json |> Result.ok_or_failwith |> fun { asks; bids; isFrozen; seq } ->
    create_books
      ~asks:(bids_asks_of_yojson Ask asks)
      ~bids:(bids_asks_of_yojson Bid bids)
      ~isFrozen:(not (isFrozen = "0"))
      ~seq ()

  let trades ?buf ~start ~stop symbol =
    let open Cohttp_async in
    let start = Time_ns.to_int_ns_since_epoch start / 1_000_000_000 |> Int.to_string in
    let stop = Time_ns.to_int_ns_since_epoch stop / 1_000_000_000 |> Int.to_string in
    let url = Uri.add_query_params' endpoint
        ["command", "returnTradeHistory";
         "currencyPair", symbol;
         "start", start;
         "end", stop;
        ]
    in
    Client.get url >>= fun (resp, body) ->
    Body.to_string body >>| fun body_str ->
    match Yojson.Safe.from_string ?buf body_str with
    | `List trades ->
      List.map trades ~f:begin fun json ->
        trade_raw_of_yojson json |> Result.ok_or_failwith |> trade_of_trade_raw
      end
    | #Yojson.Safe.json -> invalid_arg body_str
end

module Ws = struct
  type book_raw = {
    rate: string;
    typ: string [@key "type"];
    amount: (string option [@default None]);
  } [@@deriving yojson]

let book_of_book_raw { rate; typ; amount } =
  let side = match typ with "bid" -> Side.Bid | "ask" -> Ask | _ -> invalid_arg "book_of_book_raw" in
  let price = Fn.compose satoshis_int_of_float_exn Float.of_string rate in
  let qty = Option.value_map amount ~default:0 ~f:(Fn.compose satoshis_int_of_float_exn Float.of_string) in
  DB.create_book_entry side price qty ()

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

  let open_connection ?log_ws ?log ~topics () =
    let uri_str = "https://api.poloniex.com" in
    let uri = Uri.of_string uri_str in
    let host = Option.value_exn ~message:"no host in uri" Uri.(host uri) in
    let port = Option.value_exn ~message:"no port inferred from scheme"
        Uri_services.(tcp_port_of_uri uri) in
    let scheme =
      Option.value_exn ~message:"no scheme in uri" Uri.(scheme uri) in
    let buf = BytesLabels.create 4096 in
    let write_wamp w msg =
      let nb_written = Wamp_msgpck.msg_to_msgpck msg |> Msgpck.String.write buf in
      let serialized_msg = BytesLabels.sub_string buf 0 nb_written in
      maybe_debug log "-> %s" (Wamp.sexp_of_msg Msgpck.sexp_of_t msg |> Sexplib.Sexp.to_string);
      Pipe.write w serialized_msg
    in
    let read_wamp_exn msg =
      let _nb_read, msg = Msgpck.String.read msg in
      maybe_debug log "<- %s" (Msgpck.sexp_of_t msg |> Sexplib.Sexp.to_string);
      Result.ok_or_failwith @@ Wamp_msgpck.msg_of_msgpck msg
    in
    let transfer_f q =
      return @@ Queue.filter_map q ~f:(fun msg_str ->
          let _nb_read, msg = Msgpck.String.read msg_str in
          match Wamp_msgpck.msg_of_msgpck msg with
          | Ok msg -> Some msg
          | Error msg -> maybe_error log "%s" msg; None
        )
    in
    let subscribe ~topics r w =
      Deferred.List.map ~how:`Sequential topics ~f:begin fun topic ->
          let request_id, subscribe_msg = Wamp_msgpck.subscribe topic in
          write_wamp w subscribe_msg
      end
    in
    let client_r, client_w = Pipe.create () in
    let process_ws r w =
      (* Initialize *)
      maybe_info log "[WS] connected to %s" uri_str;
      let hello = Wamp_msgpck.(hello (Uri.of_string "realm1") [Subscriber]) in
      write_wamp w hello >>= fun () ->
      Pipe.read r >>= function
      | `Eof -> raise End_of_file
      | `Ok msg ->
        begin match read_wamp_exn msg with
        | Wamp.Welcome _ ->
          subscribe ~topics r w >>= fun _sub_ids ->
          Pipe.transfer' r client_w transfer_f
        | _ -> failwith "wamp: expected Welcome"
        end
    in
    let tcp_fun s r w =
      Socket.(setopt s Opt.nodelay true);
      begin
        if scheme = "https" || scheme = "wss" then Conduit_async_ssl.ssl_connect r w
        else return (r, w)
      end >>= fun (r, w) ->
      let extra_headers = Cohttp.Header.init_with "Sec-Websocket-Protocol" "wamp.2.msgpack" in
      let ws_r, ws_w =
        Websocket_async.client_ez ?log:log_ws ~opcode:Binary ~extra_headers ~heartbeat:(sec 25.) uri s r w
      in
      let cleanup () = Deferred.all_unit [Reader.close r; Writer.close w] in
      Monitor.protect ~name:"process_ws" ~finally:cleanup (fun () -> process_ws ws_r ws_w)
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
        Clock_ns.after @@ Time_ns.Span.of_int_sec 10 >>=
        loop
      end
    in
    don't_wait_for @@ loop ();
    client_r
end
