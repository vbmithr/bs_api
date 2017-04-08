open Core
open Async

open Bs_devkit

type side = [`Buy | `Sell] [@@deriving sexp]
let string_of_side = function `Buy -> "buy" | `Sell -> "sell"
let side_of_string = function
| "buy" -> `Buy
| "sell" -> `Sell
| _ -> invalid_arg "side_of string"

module Msgpck_sexp = struct
  type t = Msgpck.t =
    | Nil
    | Bool of bool
    | Int of int
    | Uint32 of int32
    | Int32 of int32
    | Uint64 of int64
    | Int64 of int64
    | Float32 of int32
    | Float of float
    | String of string
    | Bytes of string
    | Ext of int * string
    | List of t list
    | Map of (t * t) list [@@deriving sexp]
end

let margin_enabled = function
| "BTC_XMR"
| "BTC_ETH"
| "BTC_CLAM"
| "BTC_MAID"
| "BTC_FCT"
| "BTC_DASH"
| "BTC_STR"
| "BTC_BTS"
| "BTC_LTC"
| "BTC_XRP"
| "BTC_DOGE" -> true
| _ -> false

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
}

type trade = {
  ts: Time_ns.t;
  side: [`Buy | `Sell];
  price: int; (* in satoshis *)
  qty: int; (* in satoshis *)
} [@@deriving sexp]

let get_tradeID = function
  | `Null -> None
  | `Int i -> Some i
  | `String s -> Option.some @@ Int.of_string s
  | #Ezjsonm.value -> invalid_arg "get_tradeID"

let trade_encoding =
  let open Json_encoding in
  conv
    ( fun { ts ; side ; price ; qty } ->
        let price = price // 100_000_000 in
        let qty = qty // 100_000_000 in
        let total = price *. qty in
        (None, Json_repr.to_any `Null,
         Time_ns.to_string ts, string_of_side side,
         Float.to_string price,
         Float.to_string qty,
         Float.to_string total))
    (fun (globalTradeID, tradeID, date, typ, rate, amount, total) ->
       let tradeID = Json_repr.from_any tradeID |> get_tradeID in
       let id = Option.value ~default:0 tradeID in
       let ts = Time_ns.(add (of_string (date ^ "Z")) (Span.of_int_ns id)) in
       let side = side_of_string typ in
       let price = satoshis_of_string rate in
       let qty = satoshis_of_string amount in
       { ts ; side ; price ; qty })
    (obj7
       (opt "globalTradeID" any_value)
       (req "tradeID" any_value)
       (req "date" string)
       (req "type" string)
       (req "rate" string)
       (req "amount" string)
       (req "total" string))

module Rest = struct
  open Cohttp_async

  let base_uri = Uri.of_string "https://poloniex.com/public"
  let trading_uri = Uri.of_string "https://poloniex.com/tradingApi"

  let ticker_encoding symbol =
    let open Json_encoding in
    conv
      (fun { symbol; last; ask ; bid ; pct_change ; base_volume ; quote_volume ;
             is_frozen ; high24h ; low24h } ->
        let open Float in
        let last = to_string last in
        let lowestAsk = to_string ask in
        let highestBid = to_string bid in
        let percentChange = to_string pct_change in
        let baseVolume = to_string base_volume in
        let quoteVolume = to_string quote_volume in
        let isFrozen = if is_frozen then "1" else "0" in
        let high24hr = to_string high24h in
        let low24hr = to_string low24h in
        (0, last, lowestAsk, highestBid, percentChange, baseVolume,
         quoteVolume, isFrozen, high24hr, low24hr))
      (fun (id, last, lowestAsk, highestBid, percentChange, baseVolume,
            quoteVolume, isFrozen, high24hr, low24hr) -> {
          symbol ;
          last = (Float.of_string last) ;
          ask = (Float.of_string lowestAsk) ;
          bid = (Float.of_string highestBid) ;
          pct_change = (Float.of_string percentChange) ;
          base_volume = (Float.of_string baseVolume) ;
          quote_volume = (Float.of_string quoteVolume) ;
          is_frozen = (match Int.of_string isFrozen with 0 -> false | _ -> true) ;
          high24h = (Float.of_string high24hr) ;
          low24h = (Float.of_string high24hr) })
      (obj10
         (req "id" int)
         (req "last" string)
         (req "lowestAsk" string)
         (req "highestBid" string)
         (req "percentChange" string)
         (req "baseVolume" string)
         (req "quoteVolume" string)
         (req "isFrozen" string)
         (req "high24hr" string)
         (req "low24hr" string))

  let tickers () =
    let url = Uri.with_query' base_uri ["command", "returnTicker"] in
    Monitor.try_with_or_error begin fun () ->
      Client.get url >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      match Ezjsonm.from_string  body_str with
      | `O tickers ->
        List.rev_map tickers ~f:begin fun (symbol, t) ->
          Json_encoding.destruct (ticker_encoding symbol) t
        end
      | #Ezjsonm.value -> invalid_arg "tickers"
    end

  let bids_asks_of_yojson side records =
    List.map records ~f:(function
      | `List [`String price; `Int qty] ->
        DB.{ side ; price = satoshis_of_string price ; qty = qty * 100_000_000 }
      | `List [`String price; `Float qty] ->
        DB.{ side ; price = satoshis_of_string price ; qty = satoshis_int_of_float_exn qty }
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

  let books ?buf ?depth ?(symbol="all") () =
    let url = Uri.with_query' base_uri @@ List.filter_opt [
        Some ("command", "returnOrderBook");
        Some ("currencyPair", symbol);
        Option.map depth ~f:(fun lvls -> "depth", Int.to_string lvls);
      ]
    in
    Monitor.try_with_or_error begin fun () ->
      Client.get url >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      match Yojson.Safe.from_string ?buf body_str with
      | `Assoc ["error", `String msg] -> failwith msg
      | `Assoc obj as json -> begin match symbol with
        | "all" -> List.rev_map obj ~f:begin fun (symbol, b) ->
            book_raw_of_yojson b |> Result.ok_or_failwith |> fun { asks; bids; isFrozen; seq } ->
            symbol, create_books
              ~asks:(bids_asks_of_yojson `Sell asks)
              ~bids:(bids_asks_of_yojson `Buy bids)
              ~isFrozen:(not (isFrozen = "0"))
              ~seq ()
          end
        | _ ->
          book_raw_of_yojson json |> Result.ok_or_failwith |> fun { asks; bids; isFrozen; seq } ->
          [symbol, create_books
             ~asks:(bids_asks_of_yojson `Sell asks)
             ~bids:(bids_asks_of_yojson `Buy bids)
             ~isFrozen:(not (isFrozen = "0"))
             ~seq ()
          ]
        end
      | #Yojson.Safe.json -> invalid_arg "books"
    end

  let trades_exn ?log ?start_ts ?end_ts symbol =
    let start_ts_sec = Option.map start_ts ~f:begin fun ts ->
        Time_ns.to_int_ns_since_epoch ts / 1_000_000_000 |> Int.to_string
      end in
    let end_ts_sec = Option.map end_ts ~f:begin fun ts ->
        Time_ns.to_int_ns_since_epoch ts / 1_000_000_000 |> Int.to_string
      end in
    let url = Uri.add_query_params' base_uri @@ List.filter_opt Option.[
        some ("command", "returnTradeHistory");
        some ("currencyPair", symbol);
        map start_ts_sec ~f:(fun t -> "start", t);
        map end_ts_sec ~f:(fun t -> "end", t);
      ]
    in
    let decoder = Jsonm.decoder `Manual in
    let fold_trades_exn trades_w (nb_decoded, name, tmp) chunk =
      let chunk_len = String.length chunk in
      let chunk = Caml.Bytes.unsafe_of_string chunk in
      Jsonm.Manual.src decoder chunk 0 chunk_len;
      let rec decode nb_decoded name tmp =
        match Jsonm.decode decoder with
        | `Error err ->
          let err_str = Format.asprintf "%a" Jsonm.pp_error err in
          Option.iter log ~f:(fun log -> Log.error log "%s" err_str) ;
          failwith err_str
        | `Lexeme (`Float f) -> decode nb_decoded "" ((name, `Int (Float.to_int f))::tmp)
        | `Lexeme (`String s) -> decode nb_decoded "" ((name, `String s)::tmp)
        | `Lexeme (`Name name) -> decode nb_decoded name tmp
        | `Lexeme `Oe ->
          let trade = Json_encoding.destruct trade_encoding (`O tmp) in
          Pipe.write trades_w trade >>= fun () ->
          decode (succ nb_decoded) "" []

        | `Lexeme `Ae -> return (nb_decoded, name, tmp)
        | `Lexeme #Jsonm.lexeme -> decode nb_decoded name tmp
        | `Await -> return (nb_decoded, name, tmp)
        | `End -> return (nb_decoded, name, tmp)
      in
      decode nb_decoded name tmp
    in
    Option.iter log ~f:(fun log -> Log.debug log "GET %s" (Uri.to_string url)) ;
    Client.get url >>| fun (resp, body) ->
    Pipe.create_reader ~close_on_exception:false begin fun w ->
      let body_pipe = Body.to_pipe body in
      Deferred.ignore @@ Pipe.fold body_pipe
        ~init:(0, "", [])
        ~f:(fold_trades_exn w)
    end

  let all_trades_exn
      ?log
      ?(wait=Time_ns.Span.min_value)
      ?(start_ts=Time_ns.epoch)
      ?(end_ts=Time_ns.now ())
      symbol =
    let rec inner cur_end_ts w =
      trades_exn ?log ~end_ts:cur_end_ts symbol >>= fun trades ->
      let oldest_ts = ref @@ Time_ns.max_value in
      Pipe.transfer trades w ~f:(fun t -> oldest_ts := t.ts; t) >>= fun () ->
      if !oldest_ts = Time_ns.max_value || Time_ns.(!oldest_ts < start_ts) then
        (Pipe.close w; Deferred.unit)
      else
      Clock_ns.after wait >>= fun () ->
      inner Time_ns.(sub !oldest_ts @@ Span.of_int_sec 1) w
    in
    Pipe.create_reader ~close_on_exception:false (inner end_ts)

  type currency = {
    id: int;
    name: string;
    txFee: string;
    minConf: int;
    depositAddress: string option;
    disabled: int;
    delisted: int;
    frozen: int;
  } [@@deriving yojson]

  let currencies ?buf () =
    let url = Uri.add_query_params' base_uri ["command", "returnCurrencies"] in
    Monitor.try_with_or_error begin fun () ->
      Client.get url >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      match Yojson.Safe.from_string ?buf body_str with
      | `Assoc currs ->
        List.map currs ~f:begin fun (code, obj) ->
          code, currency_of_yojson obj |> Result.ok_or_failwith
        end
      | #Yojson.Safe.json -> invalid_arg "currencies"
    end

  let symbols ?buf () =
    let url = Uri.with_query' base_uri ["command", "returnOrderBook"; "currencyPair", "all"; "depth", "0"] in
    Monitor.try_with_or_error begin fun () ->
      Client.get url >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      match Yojson.Safe.from_string ?buf body_str with
      | `Assoc syms -> List.rev_map syms ~f:fst
      | #Yojson.Safe.json -> invalid_arg "symbols"
    end

  let make_sign () =
    let bigbuf = Bigstring.create 1024 in
    fun ~key ~secret ~data ->
      let nonce = Time_ns.(now () |> to_int_ns_since_epoch) / 1_000 in
      let data = ("nonce", [Int.to_string nonce]) :: data in
      let data_str = Uri.encoded_of_query data in
      let prehash = Cstruct.of_string ~allocator:(fun len -> Cstruct.of_bigarray bigbuf ~len) data_str in
      let `Hex signature = Nocrypto.Hash.SHA512.hmac ~key:secret prehash |> Hex.of_cstruct in
      data_str,
      Cohttp.Header.of_list [
        "content-type", "application/x-www-form-urlencoded";
        "Key", key;
        "Sign", signature;
      ]

  let sign = make_sign ()

  type balance_raw = {
    available: string;
    onOrders: string;
    btcValue: string;
  } [@@deriving yojson]


  type balance = {
    available: int;
    on_orders: int;
    btc_value: int;
  } [@@deriving create, sexp]

  let balance_of_balance_raw br =
    let on_orders = satoshis_of_string br.onOrders in
    let available = satoshis_of_string br.available in
    let btc_value = satoshis_of_string br.btcValue in
    create_balance ~available ~on_orders ~btc_value ()

  let balances ?buf ?(all=true) ~key ~secret () =
    let data = List.filter_opt [
        Some ("command", ["returnCompleteBalances"]);
        if all then Some ("account", ["all"]) else None
      ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      match Yojson.Safe.from_string ?buf body_str with
      | `Assoc ["error", `String msg] -> failwith msg
      | `Assoc balances -> List.Assoc.map balances ~f:begin fun b ->
          b |> balance_raw_of_yojson |> function
          | Ok br -> balance_of_balance_raw br
          | Error _ -> invalid_argf "balances: %s" body_str ()
        end
      | json -> invalid_argf "balances: %s" body_str ()
    end

  type account = Exchange | Margin | Lending [@@deriving sexp]

  let account_of_string = function
  | "exchange" -> Exchange
  | "margin" -> Margin
  | "lending" -> Lending
  | s -> invalid_argf "account_of_string: %s" s ()

  let string_of_account = function
  | Exchange -> "exchange"
  | Margin -> "margin"
  | Lending -> "lending"

  let positive_balances ?buf ~key ~secret () =
    let invarg json = invalid_argf "positive_balances: %s" (Yojson.Safe.to_string ?buf json) () in
    let data = ["command", ["returnAvailableAccountBalances"]] in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      match Yojson.Safe.from_string ?buf body_str with
      | `Assoc ["error", `String msg] -> failwith msg
      | `Assoc balances -> List.map balances ~f:begin function
        | account, `Assoc bs ->
          account_of_string account, List.Assoc.map bs ~f:begin function
          | `String bal -> satoshis_of_string bal
          | json -> invarg json
          end
        | account, json -> invarg json
        end
      | json -> invarg json
    end

  type margin_account_summary_raw = {
    totalValue: string;
    pl: string;
    lendingFees: string;
    netValue: string;
    totalBorrowedValue: string;
    currentMargin: string;
  } [@@deriving yojson]

  type margin_account_summary = {
    total_value: int [@default 0];
    pl: int [@default 0];
    lending_fees: int [@default 0];
    net_value: int [@default 0];
    total_borrowed_value: int [@default 0];
    current_margin: float [@default 0.]
  } [@@deriving create, sexp]

  let margin_account_summary_of_raw { totalValue; pl; lendingFees; netValue;
                                      totalBorrowedValue; currentMargin } =
    let total_value = satoshis_of_string totalValue in
    let pl = satoshis_int_of_float_exn @@ Float.of_string pl in
    let lending_fees = satoshis_int_of_float_exn @@ Float.of_string lendingFees in
    let net_value = satoshis_of_string netValue in
    let total_borrowed_value = satoshis_of_string totalBorrowedValue in
    let current_margin = Float.of_string currentMargin in
    create_margin_account_summary ~total_value ~pl ~lending_fees ~net_value
      ~total_borrowed_value ~current_margin ()

  let margin_account_summary ?buf ~key ~secret () =
    let data = ["command", ["returnMarginAccountSummary"]] in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      match Yojson.Safe.from_string ?buf body_str |> margin_account_summary_raw_of_yojson with
      | Error msg -> failwith body_str
      | Ok mas_raw -> margin_account_summary_of_raw mas_raw
    end

  type order_response_raw = {
    success: int [@default 1];
    message: string [@default ""];
    error: string [@default ""];
    orderNumber: string [@default ""];
    resultingTrades: Yojson.Safe.json [@default `Null];
    amountUnfilled: string [@default ""];
  } [@@deriving yojson]

  let fix_resultingTrades = function
  | `Null -> []
  | `List resulting -> List.map resulting ~f:(fun tr -> trade_raw_of_yojson tr |> Result.ok_or_failwith)
  | `Assoc [_, `List resulting] -> List.map resulting ~f:(fun tr -> trade_raw_of_yojson tr |> Result.ok_or_failwith)
  | #Ezjsonm.value -> invalid_arg "fix_resultingTrades"

  type trade_info = {
    gid: int option;
    id: int;
    trade: trade;
  } [@@deriving create, sexp]

  type order_response = {
    id: int;
    trades: trade_info list;
    amount_unfilled: int;
  } [@@deriving create, sexp]

  let order_response_encoding =
    let open Json_encoding in
    conv
      (fun { id ; trades ; amount_unfilled } ->
         (1, "", "", Int.to_string id,
          Json_repr.to_any (`A []), Int.to_string amount_unfilled))
      (fun (success)
      (obj6
         (dft "success" int 1)
         (dft "message" string "")
         (dft "error" string "")
         (dft "orderNumber" string "")
         (opt "resultingTrades" any_value)
         (dft "amountUnfilled" string ""))

  let trade_info_of_resultingTrades tr =
    let id = Option.value ~default:0 @@ get_tradeID tr.tradeID in
    let gid = get_tradeID tr.globalTradeID in
    let trade = trade_of_trade_raw tr in
    create_trade_info ?gid ~id ~trade ()

  let order_response_of_raw { success; message; error; orderNumber; resultingTrades; amountUnfilled } =
    if success = 0 then Result.fail error
    else
    let id = Int.of_string orderNumber in
    let amount_unfilled = if amountUnfilled = "" then 0 else satoshis_of_string amountUnfilled in
    let trades = fix_resultingTrades resultingTrades in
    let trades = List.map trades ~f:trade_info_of_resultingTrades in
    Result.return @@ create_order_response ~id ~trades ~amount_unfilled ()

  let order
      ?buf
      ?tif
      ?(post_only=false)
      ~key ~secret ~side ~symbol ~price ~qty () =
    let data = List.filter_opt [
        Some ("command", [match side with `Buy -> "buy" | `Sell -> "sell" ]);
        Some ("currencyPair", [symbol]);
        Some ("rate", [Float.to_string @@ price // 100_000_000]);
        Some ("amount", [Float.to_string @@ qty // 100_000_000]);
        (match tif with
        | Some `Fill_or_kill -> Some ("fillOrKill", ["1"])
        | Some `Immediate_or_cancel -> Some ("immediateOrCancel", ["1"])
        | _ -> None);
        (if post_only then Some ("postOnly", ["1"]) else None)
      ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      Yojson.Safe.from_string ?buf body_str |> function
      | `Assoc ["error", `String msg] -> failwith msg (* OK here! *)
      | resp -> match order_response_raw_of_yojson resp with
      | Ok res -> order_response_of_raw res |> Result.ok_or_failwith
      | Error _ -> failwith body_str
    end

  type cancel_response_raw = {
    success: int;
    amount: string [@default ""];
    message: string [@default ""];
    error: string [@default ""]
  } [@@deriving yojson]

  let cancel ?buf ~key ~secret id =
    let data = [
        "command", ["cancelOrder"];
        "orderNumber", [Int.to_string id];
    ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      let resp = Yojson.Safe.from_string ?buf body_str in
      let resp = cancel_response_raw_of_yojson resp |> Result.ok_or_failwith in
      if resp.success = 1 then () else failwith resp.error
    end

  let modify ?buf ?qty ~key ~secret ~price id =
    let data = List.filter_opt [
        Some ("command", ["moveOrder"]);
        Some ("orderNumber", [Int.to_string id]);
        Some ("rate", [price // 100_000_000 |> Float.to_string]);
        Option.map qty ~f:(fun a -> "amount", [a // 100_000_000 |> Float.to_string])
      ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      Yojson.Safe.from_string ?buf body_str |> function
      | resp -> match order_response_raw_of_yojson resp with
      | Ok res -> order_response_of_raw res |> Result.ok_or_failwith
      | Error _ -> failwith body_str
    end

  let margin_order
      ?buf
      ?tif
      ?(post_only=false)
      ?max_lending_rate
      ~key ~secret ~side ~symbol ~price ~qty () =
    let data = List.filter_opt [
        Some ("command", [match side with `Buy -> "marginBuy" | `Sell -> "marginSell" ]);
        Some ("currencyPair", [symbol]);
        Some ("rate", [Float.to_string @@ price // 100_000_000]);
        Some ("amount", [Float.to_string @@ qty // 100_000_000]);
        Option.map max_lending_rate ~f:(fun r -> "lendingRate", [Float.to_string r]);
        (match tif with
        | Some `Fill_or_kill -> Some ("fillOrKill", ["1"])
        | Some `Immediate_or_cancel -> Some ("immediateOrCancel", ["1"])
        | _ -> None);
        (if post_only then Some ("postOnly", ["1"]) else None)
      ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      Yojson.Safe.from_string ?buf body_str |> function
      | `Assoc ["error", `String msg] -> failwith msg (* OK here! *)
      | json -> match order_response_raw_of_yojson json with
      | Ok resp -> order_response_of_raw resp |> Result.ok_or_failwith
      | Error _ -> failwith body_str
    end

  let close_position ?buf ~key ~secret symbol =
    let data = [
      "command", ["closeMarginPosition"];
      "currencyPair", [symbol];
    ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      let resp = Yojson.Safe.from_string ?buf body_str in
      match order_response_raw_of_yojson resp with
      | Ok resp -> order_response_of_raw resp |> Result.ok_or_failwith
      | Error _ -> failwith body_str
    end

  type open_orders_resp_raw = {
    orderNumber: string;
    typ: string [@key "type"];
    rate: string;
    startingAmount: string;
    amount: string;
    total: string;
    date: string;
    margin: int;
  } [@@deriving yojson]

  type open_orders_resp = {
    id: int;
    ts: Time_ns.t;
    side: side;
    price: int;
    starting_qty: int;
    qty: int;
    margin: int;
  } [@@deriving create, sexp]

  let side_of_string = function
  | "buy" -> `Buy
  | "sell" -> `Sell
  | _ -> invalid_arg "side_of_string"

  let oo_of_oo_raw oo_raw =
    let id = Int.of_string oo_raw.orderNumber in
    let side = side_of_string oo_raw.typ in
    let price = satoshis_of_string oo_raw.rate in
    let qty = satoshis_of_string oo_raw.amount in
    let starting_qty = satoshis_of_string oo_raw.startingAmount in
    let ts = Time_ns.of_string (oo_raw.date ^ "Z") in
    let margin = oo_raw.margin in
    create_open_orders_resp ~id ~ts ~side ~price ~qty ~starting_qty ~margin ()

  let open_orders ?buf ?(symbol="all") ~key ~secret () =
    let data = [
      "command", ["returnOpenOrders"];
      "currencyPair", [symbol];
    ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    let map_f oo = oo |> open_orders_resp_raw_of_yojson |> Result.ok_or_failwith |> oo_of_oo_raw in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      Yojson.Safe.from_string ?buf body_str |> function
      | `Assoc ["error", `String msg] -> failwith msg
      | `List oos ->
        [symbol, List.map oos ~f:map_f]
      | `Assoc oo_assoc ->
        List.Assoc.map oo_assoc ~f:(function `List oos -> List.map oos ~f:map_f | #Yojson.Safe.json -> failwith body_str)
      | #Yojson.Safe.json -> failwith body_str
    end

  type trade_history_raw = {
    globalTradeID: int;
    tradeID: string;
    date: string;
    rate: string;
    amount: string;
    total: string;
    fee: string;
    orderNumber: string;
    typ: string [@key "type"];
    category: string;
  } [@@deriving yojson]

  type trade_category = Exchange | Margin | Settlement [@@deriving sexp]
  let trade_category_of_string = function
  | "exchange" -> Exchange
  | "marginTrade" -> Margin
  | "settlement" -> Settlement
  | s -> invalid_argf "trade_category_of_string: %s" s ()

  type trade_history = {
    gid: int;
    id: int;
    ts: Time_ns.t;
    price: int;
    qty: int;
    fee: int;
    order_id: int;
    side: side;
    category: trade_category
  } [@@deriving create, sexp]

  let trade_history_of_raw { globalTradeID; tradeID; date; rate; amount; total; fee; orderNumber;
                             typ; category } =
    let id = Int.of_string tradeID in
    let ts = Time_ns.of_string @@ date ^ "Z" in
    let price = satoshis_of_string rate in
    let qty = satoshis_of_string amount in
    let fee = satoshis_of_string fee in
    let order_id = Int.of_string orderNumber in
    let side = side_of_string typ in
    let category = trade_category_of_string category in
    create_trade_history ~gid:globalTradeID ~id ~ts ~price ~qty ~fee ~order_id ~side ~category ()

  let trade_history ?buf ?(symbol="all") ?start ?stop ~key ~secret () =
    let data = List.filter_opt [
        Some ("command", ["returnTradeHistory"]);
        Some ("currencyPair", [symbol]);
        Option.map start ~f:(fun ts -> "start", [Int.to_string @@ Time_ns.to_int_ns_since_epoch ts / 1_000_000_000]);
        Option.map stop ~f:(fun ts -> "end", [Int.to_string @@ Time_ns.to_int_ns_since_epoch ts / 1_000_000_000]);
    ]
    in
    let data_str, headers = sign ~key ~secret ~data in
    let map_f oo = oo |> trade_history_raw_of_yojson |> Result.ok_or_failwith |> trade_history_of_raw in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      Yojson.Safe.from_string ?buf body_str |> function
      | `Assoc ["error", `String msg] -> failwith msg
      | `List ths ->
        [symbol, List.map ths ~f:map_f]
      | `Assoc oo_assoc ->
        List.Assoc.map oo_assoc ~f:(function `List oos -> List.map oos ~f:map_f | #Yojson.Safe.json -> failwith body_str)
      | #Yojson.Safe.json -> failwith body_str
    end

  type margin_position_raw = {
    amount: string;
    total: string;
    basePrice: string;
    liquidationPrice: Yojson.Safe.json;
    pl: string;
    lendingFees: string;
    typ: string [@key "type"];
  } [@@deriving yojson]

  type margin_position = {
    price: int;
    qty: int;
    total: int;
    pl: int;
    lending_fees: int;
    liquidation_price: int option;
    side: side;
  } [@@deriving create, sexp]

  let margin_position_of_raw { amount; total; basePrice; liquidationPrice; pl; lendingFees; typ } =
    let price = satoshis_of_string basePrice in
    let qty = satoshis_int_of_float_exn @@ Float.of_string amount in
    let total = satoshis_int_of_float_exn @@ Float.of_string total in
    let pl = satoshis_int_of_float_exn @@ Float.of_string pl in
    let lending_fees = satoshis_int_of_float_exn @@ Float.of_string lendingFees in
    let liquidation_price = match liquidationPrice with
    | `String price -> Option.some @@ satoshis_int_of_float_exn @@ Float.of_string price
    | #Yojson.Safe.json -> None
    in
    let side = match typ with | "long" -> Some `Buy | "short" -> Some `Sell | _ -> None in
    Option.map side ~f:begin fun side ->
      create_margin_position ~side ~price ~qty ~total ~pl ~lending_fees ?liquidation_price ()
    end

  let margin_positions ?buf ?(symbol="all") ~key ~secret () =
    let data = ["command", ["getMarginPosition"]; "currencyPair", [symbol]] in
    let data_str, headers = sign ~key ~secret ~data in
    let filter_map_f p = p |> margin_position_raw_of_yojson |> Result.ok_or_failwith |> margin_position_of_raw in
    Monitor.try_with_or_error begin fun () ->
      Client.post ~body:(Body.of_string data_str) ~headers trading_uri >>= fun (resp, body) ->
      Body.to_string body >>| fun body_str ->
      Yojson.Safe.from_string ?buf body_str |> function
      | `Assoc ["error", `String msg] -> failwith msg
      | `Assoc (("type", _) :: a) as p -> [symbol, filter_map_f p]
      | `Assoc ps_assoc -> List.Assoc.map ps_assoc ~f:filter_map_f
      | #Yojson.Safe.json -> failwith body_str
    end
end

module Ws = struct
  type 'a t = {
    typ: string [@key "type"];
    data: 'a;
  } [@@deriving create, yojson]

  let open_connection ?(heartbeat=Time_ns.Span.of_int_sec 25) ?log_ws ?log to_ws =
    let uri_str = "https://api.poloniex.com" in
    let uri = Uri.of_string uri_str in
    let host = Option.value_exn ~message:"no host in uri" Uri.(host uri) in
    let port = Option.value_exn ~message:"no port inferred from scheme"
        Uri_services.(tcp_port_of_uri uri) in
    let scheme =
      Option.value_exn ~message:"no scheme in uri" Uri.(scheme uri) in
    let outbuf = Buffer.create 4096 in
    let write_wamp w msg =
      Buffer.clear outbuf;
      Buffer.add_bytes outbuf @@ Bytes.create 4;
      let nb_written = Wamp_msgpck.msg_to_msgpck msg |> Msgpck.StringBuf.write outbuf in
      let serialized_msg = Buffer.contents outbuf in
      Binary_packing.pack_unsigned_32_int_big_endian serialized_msg 0 nb_written;
      (* Option.iter log ~f:(fun log -> Log.debug log "-> %s" (Wamp.sexp_of_msg Msgpck.sexp_of_t msg |> Sexplib.Sexp.to_string)); *)
      Pipe.write w serialized_msg
    in
    let rec loop_write mvar msg =
      Mvar.value_available mvar >>= fun () ->
      let w = Mvar.peek_exn mvar in
      if Pipe.is_closed w then begin
        Option.iter log ~f:(fun log -> Log.error log "loop_write: Pipe to websocket closed");
        Mvar.take mvar >>= fun _ ->
        loop_write mvar msg
      end
      else write_wamp w msg
    in
    let ws_w_mvar = Mvar.create () in
    let ws_w_mvar_ro = Mvar.read_only ws_w_mvar in
    don't_wait_for @@
    Monitor.handle_errors begin fun () ->
      Pipe.iter ~continue_on_error:true to_ws ~f:(loop_write ws_w_mvar_ro)
    end
      (fun exn -> Option.iter log ~f:(fun log -> Log.error  log "%s" @@ Exn.to_string exn));
    let transfer_f q =
      let res = Queue.create () in
      let rec read_loop pos msg_str =
        if pos < String.length msg_str then
          let nb_read, msg = Msgpck.String.read ~pos:(pos+4) msg_str in
          match Wamp_msgpck.msg_of_msgpck msg with
          | Ok msg -> Queue.enqueue res msg; read_loop (pos+4+nb_read) msg_str
          | Error msg -> Option.iter log ~f:(fun log -> Log.error log "%s" msg)
      in
      Queue.iter q ~f:(read_loop 0);
      return res
    in
    let client_r, client_w = Pipe.create () in
    let process_ws r w =
      (* Initialize *)
      Option.iter log ~f:(fun log -> Log.info log "[WS] connected to %s" uri_str);
      let hello = Wamp_msgpck.(hello (Uri.of_string "realm1") [Subscriber]) in
      write_wamp w hello >>= fun () ->
      Pipe.transfer' r client_w transfer_f
    in
    let tcp_fun s r w =
      Socket.(setopt s Opt.nodelay true);
      begin
        if scheme = "https" || scheme = "wss" then Conduit_async_ssl.ssl_connect r w
        else return (r, w)
      end >>= fun (ssl_r, ssl_w) ->
      let extra_headers =
        Cohttp.Header.init_with "Sec-Websocket-Protocol" "wamp.2.msgpack.batched" in
      let ws_r, ws_w = Websocket_async.client_ez ?log:log_ws
          ~opcode:Binary ~extra_headers ~heartbeat uri s ssl_r ssl_w
      in
      let cleanup r w ws_r ws_w =
        Pipe.close_read ws_r ;
        Pipe.close ws_w ;
        Deferred.all_unit [
          Reader.close r ;
          Writer.close w ;
        ]
      in
      don't_wait_for begin
        Deferred.all_unit
          [ Reader.close_finished r ; Writer.close_finished w ] >>= fun () ->
        cleanup ssl_r ssl_w ws_r ws_w
      end ;
      Mvar.set ws_w_mvar ws_w ;
      process_ws ws_r ws_w
    in
    let rec loop () = begin
      Monitor.try_with_or_error ~name:"PNLX.Ws.open_connection"
        (fun () -> Tcp.(with_connection (to_host_and_port host port) tcp_fun)) >>| function
      | Ok () ->
        Option.iter log ~f:(fun log ->
            Log.error log "[WS] connection to %s terminated" uri_str)
      | Error err ->
        Option.iter log ~f:(fun log ->
            Log.error log "[WS] connection to %s raised %s" uri_str (Error.to_string_hum err))
    end >>= fun () ->
      if Pipe.is_closed client_r then Deferred.unit
      else begin
        Option.iter log ~f:(fun log ->
            Log.error log "[WS] restarting connection to %s" uri_str);
        Clock_ns.after @@ Time_ns.Span.of_int_sec 10 >>=
        loop
      end
    in
    don't_wait_for @@ loop ();
    client_r

  module Msgpck = struct
    type nonrec t = Msgpck.t t

    let subscribe w topics =
    let topics = List.map topics ~f:Uri.of_string in
    Deferred.List.map ~how:`Sequential topics ~f:begin fun topic ->
      let request_id, subscribe_msg = Wamp_msgpck.subscribe topic in
      Pipe.write w subscribe_msg >>| fun () ->
      request_id
    end

    let map_of_msgpck = function
    | Msgpck.Map elts ->
      List.fold_left elts ~init:String.Map.empty ~f:begin fun a -> function
      | String k, v -> String.Map.add a k v
      | _ -> invalid_arg "map_of_msgpck"
      end
    | _ -> invalid_arg "map_of_msgpck"

    let to_msgpck { typ; data } = Msgpck.(Map [String "type", String typ; String "data", data])
    let of_msgpck elts =
      try
        let elts = map_of_msgpck elts in
        let typ = String.Map.find_exn elts "type" |> Msgpck.to_string in
        let data = String.Map.find_exn elts "data" in
        Result.return (create ~typ ~data ())
      with exn -> Result.failf "%s" (Exn.to_string exn)

    let ticker_of_msgpck = function
    | Msgpck.List [String symbol; String last; String ask; String bid; String pct_change;
                   String base_volume; String quote_volume; Int is_frozen; String high24h;
                   String low24h
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
      { symbol ; last ; ask ; bid ; pct_change ; base_volume ;
        quote_volume ; is_frozen ; high24h ; low24h }
    | _ -> invalid_arg "ticker_of_msgpck"

    let trade_of_msgpck msg = try
      let msg = map_of_msgpck msg in
      let tradeID = String.Map.find_exn msg "tradeID" |> Msgpck.to_string |> Int.of_string in
      let date = String.Map.find_exn msg "date" |> Msgpck.to_string in
      let side = String.Map.find_exn msg "type" |> Msgpck.to_string in
      let rate = String.Map.find_exn msg "rate" |> Msgpck.to_string in
      let amount = String.Map.find_exn msg "amount" |> Msgpck.to_string in
      let ts = Time_ns.(add (of_string (date ^ "Z")) @@ Span.of_int_ns tradeID) in
      let side = match side with "buy" -> `Buy | "sell" -> `Sell | _ -> invalid_arg "typ_of_string" in
      let price = satoshis_of_string rate in
      let qty = satoshis_of_string amount in
      DB.{ ts ; side ; price ; qty }
    with _ -> invalid_arg "trade_of_msgpck"

    let book_of_msgpck msg = try
      let msg = map_of_msgpck msg in
      let side = String.Map.find_exn msg "type" |> Msgpck.to_string in
      let price = String.Map.find_exn msg "rate" |> Msgpck.to_string in
      let qty = String.Map.find msg "amount" |> Option.map ~f:Msgpck.to_string in
      let side = match side with "bid" -> `Buy | "ask" -> `Sell | _ -> invalid_arg "book_of_book_raw" in
      let price = satoshis_of_string price in
      let qty = Option.value_map qty ~default:0 ~f:satoshis_of_string in
      DB.{ side ; price ; qty }
    with _ -> invalid_arg "book_of_msgpck"
  end

  module Yojson = struct
    type nonrec t = Yojson.Safe.json t
    let to_yojson = to_yojson Fn.id
    let of_yojson = of_yojson (fun json -> Ok json)

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
      { symbol ; last ; ask ; bid ; pct_change ; base_volume ;
        quote_volume ; is_frozen ; high24h ; low24h }
    | json -> invalid_argf "ticker_of_json: %s" Yojson.Safe.(to_string json) ()

    type book_raw = {
      rate: string;
      typ: string [@key "type"];
      amount: (string option [@default None]);
    } [@@deriving yojson]

    let book_of_book_raw { rate; typ; amount } =
      let side = match typ with "bid" -> `Buy | "ask" -> `Sell | _ -> invalid_arg "book_of_book_raw" in
      let price = Fn.compose satoshis_int_of_float_exn Float.of_string rate in
      let qty = Option.value_map amount ~default:0 ~f:(Fn.compose satoshis_int_of_float_exn Float.of_string) in
      DB.{ side ; price ; qty }
  end
end
