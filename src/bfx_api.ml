open Core.Std
open Async.Std

open Dtc.Dtc
open Bs_devkit.Core

let exchange = "BITFINEX"

let side_of_amount amount = match Float.sign_exn amount with
  | Pos -> Buy
  | Neg -> Sell
  | Zero -> invalid_arg "side_of_amount"

module Order = struct
  type exchange_type =
    | Margin
    | Exchange

  let types_of_string = function
    | "MARKET" -> Margin, OrderType.Market, TimeInForce.Good_till_canceled
    | "LIMIT" -> Margin, Limit, Good_till_canceled
    | "STOP" -> Margin, Stop, Good_till_canceled
    | "TRAILING STOP" -> Margin, Market, Good_till_canceled
    | "FOK" -> Margin, Limit, Fill_or_kill
    | "EXCHANGE MARKET" -> Exchange, Market, Good_till_canceled
    | "EXCHANGE LIMIT" -> Exchange, Limit, Good_till_canceled
    | "EXCHANGE STOP" -> Exchange, Stop, Good_till_canceled
    | "EXCHANGE TRAILING STOP" -> Exchange, Market, Good_till_canceled
    | "EXCHANGE FOK" -> Exchange, Limit, Fill_or_kill
    | _ -> invalid_arg "Order.types_of_string"
end

module Rest = struct
  open Cohttp_async
  let base_uri = Uri.of_string "https://api.bitfinex.com"

  exception HTTP_Error of string
  exception JSON_Error of string

  let handle_rest_call (resp, body) =
    Body.to_string body >>| fun body_str ->
    let status = Response.status resp in
    match status with
    | `OK ->
      Result.try_with (fun () -> Yojson.Safe.(from_string body_str))
    | #Cohttp.Code.status_code as status ->
      Result.fail @@ HTTP_Error Cohttp.Code.(string_of_status status)

  module Sym = struct
    type t = {
      pair: string;
      price_precision: int;
      initial_margin: string;
      minimum_margin: string;
      maximum_order_size: string;
      minimum_order_size: string;
      expiration: string;
    } [@@deriving yojson]

    let get () =
      let uri = Uri.with_path base_uri "/v1/symbols_details" in
      Client.get uri >>= handle_rest_call >>| function
      | Error exn -> raise exn
      | Ok (`List syms) -> List.map syms ~f:(Fn.compose Result.ok_or_failwith of_yojson)
      | Ok #Yojson.Safe.json -> invalid_arg "get_syms"
  end

  module Trades = struct
    module Raw = struct
      type t = {
        timestamp: int;
        tid: int;
        price: string;
        amount: string;
        exchange: string;
        typ: string [@key "type"];
      } [@@deriving yojson]
    end

    type t = {
      ts: Time_ns.t;
      price: float;
      qty: float;
      side: side;
    } [@@deriving create]

    let of_raw { Raw.timestamp; price; amount; typ; _ } =
      create
        ~ts:Time_ns.(of_int_ns_since_epoch @@ timestamp * 1_000_000_000)
        ~price:Float.(of_string price)
        ~qty:Float.(of_string amount)
        ~side:(match typ with "sell" -> Sell | "buy" -> Buy | _ -> invalid_arg "side_of_string")
        ()

    let get_exn ?log ?(start=Time_ns.epoch) ?(count=50) pair =
      let q =
          ["timestamp", int_of_ts start |> Int.to_string;
           "limit_trades", Int.to_string count;
          ]
      in
      let uri = Uri.(with_query' (with_path base_uri @@ "/v1/trades/" ^ pair) q) in
      maybe_debug log "GET %s %s %d %d" pair Time_ns.(to_string start) (int_of_ts start) count;
      Client.get uri >>= handle_rest_call >>| function
      | Error exn -> raise exn
      | Ok (`List ts) ->
        let filter_map_f t =
          try begin match Raw.of_yojson t with
            | Ok raw -> Some (of_raw raw)
            | Error msg ->
              maybe_debug log "%s" msg;
              invalid_arg msg
          end
          with exn ->
            maybe_debug log "%s" (Exn.to_string exn);
            None
        in
        List.rev_filter_map ts ~f:filter_map_f
      | Ok #Yojson.Safe.json -> invalid_arg "Rest.Trades.get"
  end

  module Priv = struct
    let post_exn ?buf ?log ~key ~secret ~endp ~body () =
      let open Nocrypto in
      maybe_debug log "-> %s" Yojson.Safe.(to_string body);
      let uri = Uri.with_path base_uri endp in
      let nonce = Time_ns.(now () |> to_int63_ns_since_epoch) |> Int63.to_string in
      let payload =
        match body with
        | `Assoc params ->
          `Assoc (["request", `String endp;
                   "nonce", `String nonce;
                  ] @ params)
        | _ -> invalid_arg "bitfinex post body must be a json dict"
      in
      let body = Yojson.Safe.to_string ?buf payload in
      let body_b64 = Base64.encode Cstruct.(of_string body) in
      let signature = Hash.SHA384.hmac ~key:secret body_b64 in
      let signature = Hex.of_cstruct signature in
      let headers = Cohttp.Header.of_list
          ["X-BFX-APIKEY", key;
           "X-BFX-PAYLOAD", Cstruct.to_string body_b64;
           "X-BFX-SIGNATURE", match signature with `Hex sign -> sign;
          ] in
      let body = Cohttp_async.Body.of_string body in
      Client.post ~headers ~body uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body ->
      maybe_debug log "<- %s" body;
      Yojson.Safe.from_string ?buf body

    module Order = struct
      let string_of_buy_sell = function
      | Buy -> "buy"
      | Sell -> "sell"

      let string_of_ord_type_tif ord_type tif = match ord_type, tif with
      | Some OrderType.Market, _ -> "market"
      | Some Stop, _ -> "stop"
      | Some Limit, Some TimeInForce.Day
      | Some Limit, Some Good_till_canceled -> "limit"
      | Some Limit, Some Fill_or_kill -> "fill-or-kill"
      | _, _ -> invalid_arg "string_of_ord_type_tif"

      let submit_exn ?buf ?log ~key ~secret o =
        let open Trading.Order in
        let side = Option.value_exn ~message:"side is unset" o.Submit.side in
        let side = string_of_buy_sell side in
        let typ = string_of_ord_type_tif o.ord_type o.tif in
        let price = if o.ord_type = Some Market then 1. else o.p1 in
        let body = `Assoc [
            "symbol", `String o.Submit.symbol;
            "exchange", `String "bitfinex";
            "price", `String (Printf.sprintf "%.5f" price);
            "amount", `String (Printf.sprintf "%.2f" o.qty);
            "side", `String side;
            "type", `String typ;
          ]
        in
        post_exn ?buf ?log ~key ~secret ~endp:"/v1/order/new" ~body

      let cancel_exn ?buf ?log ~key ~secret oid =
        let body = `Assoc ["order_id", `Int oid] in
        post_exn ?buf ?log ~key ~secret ~endp:"/v1/order/cancel" ~body

      module Response = struct
        module Raw = struct
          type t = {
            id: int;
            symbol: string;
            exchange: string;
            price: string;
            avg_execution_price: string;
            side: string;
            typ: string [@key "type"];
            timestamp: string;
            is_live: bool;
            is_canceled: bool [@key "is_cancelled"];
            is_hidden: bool;
            oco_order: int option;
            was_forced: bool;
            original_amount: string;
            remaining_amount: string;
            executed_amount: string;
            order_id: int;
          } [@@deriving yojson]
        end

        type t = {
          id: int;
          symbol: string;
          exchange: string;
          price: float;
          avg_execution_price: float;
          side: side;
          typ: OrderType.t;
          tif:TimeInForce.t;
          ts: Time_ns.t;
          is_live: bool;
          is_hidden: bool;
          is_canceled: bool;
          oco_order: int option;
          was_forced: bool;
          original_amount: float;
          remaining_amount: float;
          executed_amount: float;
        } [@@deriving create]

        let of_raw r =
          let price = Float.of_string r.Raw.price in
          let avg_execution_price = Float.of_string r.avg_execution_price in
          let original_amount = Float.of_string r.original_amount in
          let remaining_amount = Float.of_string r.remaining_amount in
          let executed_amount = Float.of_string r.executed_amount in
          let side = match r.side with
            | "buy" -> Buy
            | "sell" -> Sell
            | _ -> invalid_arg "side_of_string"
          in
          let _, typ, tif = Order.types_of_string @@ String.uppercase r.typ in
          let ts = match String.split r.timestamp ~on:'.' with
            | [s; ns] ->
              Int.(of_string s) * 1_000_000_000 + Int.(of_string ns) |>
              Time_ns.of_int_ns_since_epoch
            | _ -> invalid_arg "timestamp"
          in
          create
            ~id:r.id
            ~symbol:r.symbol
            ~exchange:r.exchange
            ~price
            ~avg_execution_price
            ~side
            ~typ
            ~tif
            ~ts
            ~is_live:r.is_live
            ~is_hidden:r.is_hidden
            ~is_canceled:r.is_canceled
            ?oco_order:r.oco_order
            ~was_forced:r.was_forced
            ~original_amount
            ~remaining_amount
            ~executed_amount
            ()
        let of_yojson json =
          Raw.of_yojson json |> function
          | Error msg -> invalid_arg "Rest.Priv.Order.Response.of_yojson"
          | Ok r -> of_raw r
      end
    end
  end
end

module Ws = struct
  let float_of_json = function
    | `Int v -> Float.of_int v
    | `Float v -> v
    | `Intlit v -> Float.of_string v
    | #Yojson.Safe.json as json ->
      invalid_arg Printf.(sprintf "float_of_json: %s" Yojson.Safe.(to_string json))

  let bool_of_int = function
    | 1 -> true
    | 0 -> false
    | _ -> invalid_arg "Ws.bool_of_int"

  let maybe_int = function
    | `Int i -> Some i
    | `Null -> None
    | #Yojson.Safe.json -> invalid_arg "Ws.maybe_int"

  module Ev = struct
    type t = {
      name: string;
      fields: Yojson.Safe.json String.Map.t
    }

    let create ~name ~fields () = { name; fields = String.Map.of_alist_exn fields }

    let of_yojson = function
    | `Assoc fields when List.Assoc.mem fields "event" ->
      create
        ~name:List.Assoc.(find_exn fields "event" |> Yojson.Safe.to_basic |> Yojson.Basic.Util.to_string)
        ~fields:List.Assoc.(remove fields "event")
        () |> Result.return
    | #Yojson.Safe.json as s -> Result.failf "%s" @@ Yojson.Safe.to_string s

    let to_yojson { name; fields } =
      `Assoc (("event", `String name) :: String.Map.(to_alist fields))
  end

  module Msg = struct
    type channel =
      | Book
      | Trades
      | Ticker

    let channel_of_string = function
      | "book" -> Book
      | "trades" -> Trades
      | "ticker" -> Ticker
      | _ -> invalid_arg "channel_of_string"

    let channel_to_string = function
      | Book -> "book"
      | Trades -> "trades"
      | Ticker -> "ticker"

    type chan_descr = {
      chan: channel;
      pair: string;
    } [@@deriving create]

    type t = {
      chan: int;
      msg: Yojson.Safe.json list
    } [@@deriving create]

    let of_yojson = function
      | `List (`Int chan :: msg) -> create ~chan ~msg ()
      | #Yojson.Safe.json -> invalid_arg "Msg.on_yojson"
  end

  module Ticker = struct
    type t = {
      ts: Time_ns.t;
      bid: float;
      bidSize: float;
      ask: float;
      askSize: float;
      dailyChg: float;
      dailyChgPct: float;
      last: float;
      vol: float;
      high: float;
      low: float;
    } [@@deriving create]

    let of_yojson ~ts msg =
      match List.map msg ~f:float_of_json with
      | [ bid; bidSize; ask; askSize; dailyChg; dailyChgPct; last; vol; high; low] ->
        create ts bid bidSize ask askSize dailyChg dailyChgPct last vol high low ()
      | _ -> invalid_arg "Ticker.of_yojson"
  end

  module Book = struct
    module Raw = struct
      type t = {
        id: int;
        price: float;
        amount: float;
      } [@@deriving show,create]

      let of_yojson msg =
        match List.map msg ~f:float_of_json with
        | [id; price; amount] -> create ~id:(Int.of_float id) ~price ~amount ()
        | _ -> invalid_arg "Book.Raw.of_yojson"
    end

    type t = {
      price: float;
      count: int;
      amount: float;
    } [@@deriving create]

    let of_yojson msg =
      match List.map msg ~f:float_of_json with
      | [price; count; amount] -> create ~price ~count:Int.(of_float count) ~amount ()
      | _ -> invalid_arg "Book.of_yojson"

  end

  module Trade = struct
    type t = {
      ts: Time_ns.t;
      price: float;
      amount: float;
    } [@@deriving create]

    let of_yojson = function
      | (`String _) :: tl
      | (`Int _) :: tl -> begin
        match List.map tl ~f:float_of_json with
        | [ ts; price; amount ] ->
          let ts = Int.(of_float ts) * 1_000_000_000 |> Time_ns.of_int_ns_since_epoch in
          create ~ts ~price ~amount ()
        | _ -> invalid_arg "Trade.of_yojson"
        end
      | _ -> invalid_arg "Trade.of_yojson"
  end

  module Priv = struct
    type update_type =
      | Snapshot
      | Update
      | New
      | Cancel
      | Execution
      [@@deriving show]

    type msg_type =
      | Heartbeat
      | Position
      | Wallet
      | Order
      | HistoricalOrder
      | Trade
      [@@deriving show]

    let types_of_msg : string -> msg_type * update_type = function
      | "hb" -> Heartbeat, Update
      | "ps" -> Position, Snapshot
      | "ws" -> Wallet, Snapshot
      | "os" -> Order, Snapshot
      | "hos" -> HistoricalOrder, Snapshot
      | "ts" -> Trade, Snapshot
      | "on" -> Order, New
      | "ou" -> Order, Update
      | "oc" -> Order, Cancel
      | "pn" -> Position, New
      | "pu" -> Position, Update
      | "pc" -> Position, Cancel
      | "wu" -> Wallet, Update
      | "te" -> Trade, Execution
      | "tu" -> Trade, Update
      | _ -> invalid_arg "Ws.Priv.types_of_msg"

    module Position = struct
      type t = {
        pair: string;
        status: [`Active | `Closed];
        amount: float;
        base_price: float;
        margin_funding: float;
        funding_type: [`Daily | `Term]
      } [@@deriving show,create]

      let of_yojson = function
        | `List [`String pair; `String status; amount; base_price;
           margin_funding; `Int funding_type] ->
          let amount = float_of_json amount in
          let base_price = float_of_json base_price in
          let margin_funding = float_of_json margin_funding in
          let status = match status with "ACTIVE" -> `Active | _ -> `Closed in
          let funding_type = match funding_type with 0 -> `Daily | _ -> `Term in
          create ~pair ~status ~amount ~base_price ~margin_funding ~funding_type ()
        | #Yojson.Safe.json -> invalid_arg "Position.of_yojson"
    end

    module Wallet = struct
      type t = {
        name: [`Trading | `Exchange | `Deposit];
        currency: string;
        balance: float;
        interest_unsettled: float
      } [@@deriving create]

      let name_of_string = function
        | "trading" -> `Trading
        | "exchange" -> `Exchange
        | "deposit" -> `Deposit
        | _ -> invalid_arg "name_of_string"

      let of_yojson = function
        | `List [ `String name; `String currency; balance; interest_unsettled] ->
          let balance = float_of_json balance in
          let interest_unsettled = float_of_json interest_unsettled in
          let name = name_of_string name in
          create ~name ~currency ~balance ~interest_unsettled ()
        | #Yojson.Safe.json as json ->
          invalid_arg Printf.(sprintf "Wallet.of_yojson %s" Yojson.Safe.(to_string json))
    end

    module Order = struct
      include Order
      let status_of_string status =
        let words = String.split status ~on:' ' in
        match List.hd words with
        | None -> invalid_arg "Order.status_of_string"
        | Some "ACTIVE" -> OrderStatus.Open
        | Some "EXECUTED" -> Filled
        | Some "PARTIALLY" -> Partially_filled
        | Some "CANCELED" -> Canceled
        | Some _ -> invalid_arg "Order.status_of_string"

      type t = {
        id: int;
        pair: string;
        amount: float;
        amount_orig: float;
        typ: OrderType.t;
        tif:TimeInForce.t;
        exchange_typ:Order.exchange_type;
        status: OrderStatus.t;
        price: float;
        avg_price: float;
        created_at: Time_ns.t;
        notify: bool;
        hidden: bool;
        oco: int option;
      } [@@deriving create]

      let of_yojson = function
        | `List [`Int id; `String pair; amount; amount_orig;
           `String typ; `String status; price; avg_price;
           `String created_at; `Int notify; `Int hidden; oco] ->
          let amount = float_of_json amount in
          let amount_orig = float_of_json amount_orig in
          let price = float_of_json price in
          let avg_price = float_of_json avg_price in
          let exchange_typ, typ, tif = Order.types_of_string typ in
          let status = status_of_string status in
          let created_at = Time_ns.of_string created_at in
          let notify = bool_of_int notify in
          let hidden = bool_of_int hidden in
          let oco = maybe_int oco in
          create ~id ~pair ~amount ~amount_orig ~typ ~tif ~exchange_typ
            ~status ~price ~avg_price ~created_at ~notify ~hidden ?oco ()
        | #Yojson.Safe.json -> invalid_arg "Order.of_yojson"
    end
    module Trade = struct
      type t = {
        id: int;
        pair: string;
        ts: Time_ns.t;
        order_id: int;
        amount: float;
        price: float;
        exchange_typ: Order.exchange_type;
        typ: OrderType.t;
        tif: TimeInForce.t;
        order_price: float;
        fee: float;
        fee_currency: string;
      } [@@deriving create]

      let of_yojson = function
        | `List [ `Int id; `String pair; `Int ts; `Int order_id; amount; price; `String typ; order_price; fee; `String fee_currency]
        | `List [ `String _; `Int id; `String pair; `Int ts; `Int order_id; amount; price; `String typ; order_price; fee; `String fee_currency] ->
          let ts = Time_ns.of_int_ns_since_epoch (ts * 1_000_000_000) in
          let exchange_typ, typ, tif = Order.types_of_string typ in
          let amount = float_of_json amount in
          let price = float_of_json price in
          let order_price = float_of_json order_price in
          let fee = float_of_json fee in
          create ~id ~pair ~ts ~order_id ~amount ~price
            ~exchange_typ ~typ ~tif ~order_price ~fee ~fee_currency ()
        | #Yojson.Safe.json -> invalid_arg "Trade.of_yojson"
    end
  end

  type auth = {
    event: string;
    apiKey: string;
    authSig: string;
    authPayload: string;
  } [@@deriving create,yojson]

  let sign ~key:apiKey ~secret =
    let open Nocrypto in
    let payload = "AUTH" ^ Time_ns.(now () |> Time_ns.to_int_ns_since_epoch |> fun t -> t / 1_000_000 |> Int.to_string) in
    let `Hex authSig = Hash.SHA384.hmac ~key:secret Cstruct.(of_string payload) |> Hex.of_cstruct in
    create_auth ~event:"auth" ~apiKey ~authSig ~authPayload:payload ()

  let open_connection ?(buf=Bi_outbuf.create 4096) ?auth ?log ?to_ws () =
    let uri_str = "https://api2.bitfinex.com:3000/ws" in
    let uri = Uri.of_string uri_str in
    let uri_str = Uri.to_string uri in
    let host = Option.value_exn ~message:"no host in uri" Uri.(host uri) in
    let port = Option.value_exn ~message:"no port inferred from scheme"
        Uri_services.(tcp_port_of_uri uri)
    in
    let scheme = Option.value_exn ~message:"no scheme in uri" Uri.(scheme uri) in
    let cur_ws_w = ref None in
    Option.iter to_ws ~f:begin fun to_ws -> don't_wait_for @@
      Monitor.handle_errors (fun () ->
          Pipe.iter ~continue_on_error:true to_ws ~f:begin fun ev ->
            let ev_str = (ev |> Ev.to_yojson |> Yojson.Safe.to_string) in
            maybe_debug log "-> %s" ev_str;
            match !cur_ws_w with
            | None -> Deferred.unit
            | Some w -> Pipe.write_if_open w ev_str
          end
        )
        (fun exn -> maybe_error log "%s" @@ Exn.to_string exn)
    end;
    let client_r, client_w = Pipe.create () in
    let tcp_fun s r w =
      Socket.(setopt s Opt.nodelay true);
      begin if scheme = "https" || scheme = "wss" then Conduit_async_ssl.ssl_connect r w
      else return (r, w)
      end >>= fun (r, w) ->
      let ws_r, ws_w = Websocket_async.client_ez uri s r w in
      cur_ws_w := Some ws_w;
      let cleanup () =
        Pipe.close_read ws_r;
        Deferred.all_unit [Reader.close r; Writer.close w]
      in
      maybe_info log "[WS] connecting to %s" uri_str;
      (* AUTH *)
      begin match auth with
      | None -> Deferred.unit
      | Some (key, secret) ->
        let auth = sign key secret in
        let auth_str = (auth |> auth_to_yojson |> Yojson.Safe.to_string) in
        maybe_debug log "-> %s" auth_str;
        Pipe.write ws_w auth_str
      end >>= fun () ->
      Monitor.protect ~finally:cleanup (fun () -> Pipe.transfer ws_r client_w ~f:(Yojson.Safe.from_string ~buf))
    in
    let rec loop () = begin
      Monitor.try_with_or_error ~name:"BFX.Ws.with_connection"
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
