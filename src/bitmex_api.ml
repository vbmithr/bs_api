open Core.Std
open Async.Std

open Dtc
open Bs_devkit.Core

let uri = Uri.of_string "https://www.bitmex.com"
let testnet_uri = Uri.of_string "https://testnet.bitmex.com"

module Instrument = struct
  open RespObj
  let is_index symbol = symbol.[0] = '.'
  let to_secdef ~testnet t =
    let symbol = string_exn t "symbol" in
    let index = is_index symbol in
    let exchange =
      string_exn t "reference"
      ^ (if testnet && not index then "T" else "")
    in
    let tickSize = float_exn t "tickSize" in
    let open SecurityDefinition in
    Response.create
      ~symbol
      ~exchange
      ~security_type:(if index then `Index else `Future)
      ~descr:""
      ~min_price_increment:tickSize
      ~price_display_format:(price_display_format_of_ticksize tickSize)
      ~currency_value_per_increment:tickSize
      ~underlying_symbol:(string_exn t "underlyingSymbol")
      ~updates_bid_ask_only:false
      ~has_market_depth_data:(not index)
      ?expiration_date:Option.(
          map (string t "expiry")
            ~f:(fun time -> Time.(of_string time |> to_epoch |> Int32.of_float)))
      ()
end

module OrderBook = struct
  module Deprecated = struct
    type t = {
      symbol: string;
      level: int;
      bidSize: (int option [@default None]);
      bidPrice: (float option [@default None]);
      askSize: (int option [@default None]);
      askPrice: (float option [@default None]);
      timestamp: string [@default Core.Std.Time_ns.(epoch |> to_string)];
    } [@@deriving show,yojson]
  end

  module L2 = struct
    type t = {
      symbol: string;
      id: int;
      side: string;
      size: int option [@default None];
      price: float option [@default None];
    } [@@deriving yojson]
  end

  module T = struct
    type t = {
      price: Float.t [@default 0.];
      size: Int.t [@default 0];
      ts: Time_ns.t [@default Time_ns.epoch];
    } [@@deriving sexp,create]
    let compare a b = match Float.compare a.price b.price with
      | 0 -> Int.compare a.size b.size
      | n -> n
  end
  include T
  module Set = Set.Make(T)
end

module Quote = struct
  type t = {
    timestamp: string;
    symbol: string;
    bidPrice: float option;
    bidSize: int option;
    askPrice: float option;
    askSize: int option;
  } [@@deriving show,create,yojson]

  let merge t t' =
    if t.symbol <> t'.symbol then invalid_arg "Quote.merge: symbols do not match";
    let merge_quote = Option.merge ~f:(fun _ q' -> q') in
    create
      ~timestamp:t'.timestamp
      ~symbol:t'.symbol
      ?bidPrice:(merge_quote t.bidPrice t'.bidPrice)
      ?bidSize:(merge_quote t.bidSize t'.bidSize)
      ?askPrice:(merge_quote t.askPrice t'.askPrice)
      ?askSize:(merge_quote t.askSize t'.askSize)
      ()
end

module Crypto = struct
  let nonce = ref Time_ns.(now () |> to_int_ns_since_epoch |> fun t -> t / 1_000_000)

  let sign ?log ?(data="") ~secret ~verb ~endp () =
    let verb_str = match verb with
      | `GET -> "GET"
      | `POST -> "POST"
      | `PUT -> "PUT"
      | `DELETE -> "DELETE"
    in
    incr nonce;
    let nonce = Int.to_string !nonce in
    maybe_debug log "sign %s" nonce;
    let prehash = verb_str ^ endp ^ nonce ^ data in
    match Hex.(of_cstruct Nocrypto.Hash.SHA256.(hmac ~key:secret Cstruct.(of_string prehash))) with `Hex sign -> nonce, sign

  let mk_query_params ?log ?(data="") ~key ~secret verb uri =
    let endp = Uri.path_and_query uri in
    let nonce, signature = sign ?log ~secret ~verb ~endp ~data () in
    [ "api-nonce", [nonce];
      "api-key", [key];
      "api-signature", [signature];
    ]
end

module Rest = struct
  module C = Cohttp
  open Cohttp_async

  type error_content = {
    name: string;
    message: string;
  } [@@deriving show,yojson]

  type error = {
    error: error_content
  } [@@deriving yojson]

  let handle_rest_error ?log ~name (resp, body) =
    let rec inner () =
      Body.to_string body >>= fun body_str ->
      let status = Response.status resp in
      let status_code = C.Code.code_of_status status in
      if C.Code.is_success status_code then begin
        maybe_debug log "<- %s" body_str;
        return body_str
      end
      else if C.Code.is_client_error status_code then begin
        match error_of_yojson Yojson.Safe.(from_string body_str) with
        | `Ok { error = { name; message }} ->
          failwithf "%s: %s" name message ()
        | `Error _ ->
          failwithf "%s: json error" name ()
      end
      else if C.Code.is_server_error status_code then begin
        maybe_error log "%s: %s" name (C.Code.sexp_of_status_code status |> Sexplib.Sexp.to_string_hum);
        after @@ Time.Span.of_int_sec 1 >>=
        inner
      end
      else failwithf "%s: Unexpected HTTP return status %s"
          name (C.Code.sexp_of_status_code status |> Sexplib.Sexp.to_string_hum) ()
    in
    inner ()

  let mk_headers ?log ?(data="") ~key ~secret verb uri =
    let query_params = Crypto.mk_query_params ?log ~data ~key ~secret verb uri in
    Cohttp.Header.of_list @@
    ("content-type", "application/json") :: List.Assoc.map query_params ~f:List.hd_exn

  module Order = struct
    let submit ?log ~testnet ~key ~secret orders =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/bulk" in
      let body_str = Yojson.Safe.to_string @@ `Assoc ["orders", `List orders] in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `POST uri in
      maybe_debug log "-> %s" body_str;
      Client.post ~chunked:false ~body ~headers uri >>=
      handle_rest_error ?log ~name:"submit"

    let update ?log ~testnet ~key ~secret orders =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/bulk" in
      let body_str = Yojson.Safe.to_string @@ `Assoc ["orders", `List orders] in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `PUT uri in
      maybe_debug log "-> %s" body_str;
      Client.put ~chunked:false ~body ~headers uri >>=
      handle_rest_error ?log ~name:"update"

    let cancel ?log ~testnet ~key ~secret orderID =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order" in
      let body_str = `Assoc ["orderID", `String Uuid.(to_string orderID)] |> Yojson.Safe.to_string in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `DELETE uri in
      maybe_debug log "-> %s" body_str;
      Client.delete ~chunked:false ~body ~headers uri >>=
      handle_rest_error ?log ~name:"cancel"

    let cancel_all ?log ?symbol ?filter ~testnet ~key ~secret () =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/all" in
      let body = `Assoc begin
          List.filter_opt
            [Option.map filter ~f:(fun json -> "filter", json);
             Option.map symbol ~f:(fun sym -> "symbol", `String sym);
            ]
        end
      in
      let body_str = Yojson.Safe.to_string body in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `DELETE uri in 
      maybe_debug log "-> %s" body_str;
      Client.delete ~chunked:false ~body ~headers uri >>=
      handle_rest_error ?log ~name:"cancel_all"

    let cancel_all_after ?log ~testnet ~key ~secret timeout =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/cancelAllAfter" in
      let body_str = `Assoc ["timeout", `Int timeout] |> Yojson.Safe.to_string in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `POST uri in
      maybe_debug log "-> %s" body_str;
      Client.post ~chunked:false ~body ~headers uri >>=
      handle_rest_error ?log ~name:"cancel_all_after"
  end
end

module Ws = struct
  module Yojson = struct
    module Safe = struct
      include Yojson.Safe
      let pp_json fmt json = Format.fprintf fmt "%s" @@ to_string json
      let show_json json = Format.(fprintf str_formatter "%a" pp_json json)
    end
  end

  type error = {
    error: string;
  } [@@deriving yojson]

  type response = {
    success: bool;
    symbol: (string [@default ""]);
    subscribe: (string [@default ""]);
    account: (int [@default 0]);
    request: Yojson.Safe.json;
  } [@@deriving show, yojson]

  type update = {
    table: string;
    keys: (string list [@default []]);
    types: (Yojson.Safe.json [@default `Null]);
    foreignKeys: (Yojson.Safe.json [@default `Null]);
    attributes: (Yojson.Safe.json [@default `Null]);
    action: string;
    data: Yojson.Safe.json list;
  } [@@deriving show, yojson]

  type query = {
    op: string;
    args: Yojson.Safe.json;
  } [@@deriving create,yojson]

  let uri = Uri.with_path uri "realtime"
  let testnet_uri = Uri.with_path testnet_uri "realtime"

  let with_connection ?(stop_f=Fn.const false) ?(query_params=[]) ?log ?auth ?to_ws ~testnet ~topics ~on_ws_msg () =
    let uri = if testnet then testnet_uri else uri in
    let auth_params = match auth with
      | None -> []
      | Some (key, secret) -> Crypto.mk_query_params ?log ~key ~secret `GET uri
    in
    let uri = Uri.add_query_params uri @@
      ["heartbeat", ["true"];
       "subscribe", topics
      ] @ auth_params @ query_params
    in
    let uri_str = Uri.to_string uri in
    let host = Option.value_exn ~message:"no host in uri" Uri.(host uri) in
    let port = Option.value_exn ~message:"no port inferred from scheme"
        Uri_services.(tcp_port_of_uri uri)
    in
    let scheme = Option.value_exn ~message:"no scheme in uri" Uri.(scheme uri) in
    let tcp_fun s r w =
      Socket.(setopt s Opt.nodelay true);
      begin
        if scheme = "https" || scheme = "wss" then Conduit_async_ssl.ssl_connect r w
        else return (r, w)
      end >>= fun (r, w) ->
      let ws_r, ws_w = Websocket_async.client_ez ?log ~heartbeat:(sec 25.) uri s r w in
      Option.iter to_ws ~f:(fun to_ws ->
          don't_wait_for @@ Pipe.transfer to_ws ws_w ~f:(fun q ->
              let q_str = (q |> query_to_yojson |> Yojson.Safe.to_string) in
              maybe_debug log "-> %s" q_str;
              q_str
            )
        );
      let cleanup () =
        Pipe.close_read ws_r;
        Deferred.all_unit [Reader.close r; Writer.close w]
      in
      maybe_info log "[WS] connecting to %s" uri_str;
      let pipe_f msg = try on_ws_msg msg with exn -> maybe_error log "%s" (Exn.to_string exn) in
      Monitor.protect ~finally:cleanup (fun () -> Pipe.iter_without_pushback ws_r ~f:pipe_f)
    in
    let rec loop () =
      begin
        Monitor.try_with_or_error ~name:"with_connection" (fun () -> Tcp.(with_connection (to_host_and_port host port) tcp_fun)) >>| function
        | Ok () -> maybe_error log "[WS] connection to %s terminated" uri_str;
        | Error err -> maybe_error log "[WS] connection to %s raised %s" uri_str (Error.to_string_hum err)
      end >>= fun () ->
      if stop_f () then Deferred.unit
      else begin
        maybe_error log "[WS] restarting connection to %s" uri_str;
        after @@ sec 10. >>= loop
      end
    in
    loop ()
end

module Trade = struct
  type t = {
    symbol: string;
    timestamp: string;
    price: float;
    size: int64;
    side: string;
    tickDirection: (string option [@default None]);
    trdMatchID: (string option [@default None]);
    grossValue: (float option [@default None]);
    homeNotional: (float option [@default None]);
    foreignNotional: (float option [@default None]);
    id: (float option [@default None]);
  } [@@deriving show,yojson]
end

let string_of_ord_type = function
  | `Market -> "Market"
  | `Limit -> "Limit"
  | `Stop -> "Stop"
  | `Stop_limit -> "StopLimit"
  | `Market_if_touched -> "MarketIfTouched"
  | #order_type -> invalid_arg "string_of_ord_type"

let ord_type_of_string = function
  | "Market" -> `Market
  | "Limit" -> `Limit
  | "Stop" -> `Stop
  | "StopLimit" -> `Stop_limit
  | "MarketIfTouched" -> `Market_if_touched
  | _ -> invalid_arg "ord_type_of_string"

let string_of_tif = function
  | `Day -> "Day"
  | `Good_till_canceled | `All_or_none -> "GoodTillCancel"
  | `Immediate_or_cancel -> "ImmediateOrCancel"
  | `Fill_or_kill -> "FillOrKill"
  | #time_in_force -> invalid_arg "string_of_tif"

let tif_of_string = function
  | "Day" -> `Day
  | "GoodTillCancel" -> `Good_till_canceled
  | "ImmediateOrCancel" -> `Immediate_or_cancel
  | "FillOrKill" -> `Fill_or_kill
  | _ -> invalid_arg "tif_of_string"

let p1_p2_of_bitmex ~ord_type ~stopPx ~price = match ord_type with
  | `Market -> None, None
  | `Limit -> Some price, None
  | `Stop -> Some stopPx, None
  | `Stop_limit -> Some stopPx, Some price
  | `Market_if_touched -> Some stopPx, None

let string_of_stop_exec_inst = function
  | `MarkPrice -> "MarkPrice"
  | `LastPrice -> "LastPrice"

let price_fields_of_dtc ?p1 ?p2 ord_type =
  match ord_type with
  | `Unset | `Market -> []
  | `Limit -> (match p1 with None -> [] | Some p1 -> ["price", `Float p1])
  | `Stop | `Market_if_touched ->
    (match p1 with
     | None -> invalid_arg "price_field_of_dtc" (* Cannot happen *)
     | Some p1 -> ["stopPx", `Float p1]
    )
  | `Stop_limit ->
    List.filter_opt
      [Option.map p1 ~f:(fun p1 -> "stopPx", `Float p1);
       Option.map p2 ~f:(fun p2 -> "price", `Float p2);
      ]

let execInst_of_dtc ord_type tif stop_exec_inst =
  let sei_str = string_of_stop_exec_inst stop_exec_inst in
  let execInst = match ord_type with
    | `Stop | `Market_if_touched | `Stop_limit -> [sei_str]
    | #Dtc.order_type -> []
  in match tif with
  | `All_or_none -> ["execInst", `String (String.concat ~sep:"," ("AllOrNone" :: execInst)); "displayQty", `Float 0.]
  | #Dtc.time_in_force -> List.map execInst ~f:(fun ei -> "execInst", `String ei)

type update_action =
  | Partial [@printer fun fmt _ -> Format.pp_print_string fmt "Partial"]
  | Insert [@printer fun fmt _ -> Format.pp_print_string fmt "Insert"]
  | Update [@printer fun fmt _ -> Format.pp_print_string fmt "Update"]
  | Delete [@printer fun fmt _ -> Format.pp_print_string fmt "Delete"]
  [@@deriving show]

let update_action_of_string = function
  | "partial" -> Partial
  | "insert" -> Insert
  | "update"  -> Update
  | "delete" -> Delete
  | _ -> invalid_arg "update_action_of_bitmex"

let buy_sell_of_bmex = function
  | "Buy" -> `Buy
  | "Sell" -> `Sell
  | _ -> invalid_arg "buy_sell_of_bmex"

let bmex_of_buy_sell = function
  | `Buy -> "Buy"
  | `Sell -> "Sell"
