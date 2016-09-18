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
    let open Dtc.SecurityDefinition in
    Response.create
      ~symbol
      ~exchange
      ~security_type:(if index then Index else Futures)
      ~descr:""
      ~min_price_increment:tickSize
      ~price_display_format:(Dtc.price_display_format_of_ticksize tickSize)
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
  let gen_nonce = function
  | `Rest -> Time_ns.(now () |> to_int_ns_since_epoch) / 1_000_000_000 + 5
  | `Ws -> Time_ns.(now () |> to_int_ns_since_epoch) / 1_000

  let sign ?log ?(data="") ~secret ~verb ~endp kind =
    let verb_str = match verb with
      | `GET -> "GET"
      | `POST -> "POST"
      | `PUT -> "PUT"
      | `DELETE -> "DELETE"
    in
    let nonce = gen_nonce kind in
    let nonce_str = Int.to_string nonce in
    Option.iter log ~f:(fun log -> Log.debug log "sign %s" nonce_str);
    let prehash = verb_str ^ endp ^ nonce_str ^ data in
    match Hex.(of_cstruct Nocrypto.Hash.SHA256.(hmac ~key:secret Cstruct.(of_string prehash))) with `Hex sign ->
      nonce, sign

  let mk_query_params ?log ?(data="") ~key ~secret kind verb uri =
    let endp = Uri.path_and_query uri in
    let nonce, signature = sign ?log ~secret ~verb ~endp ~data kind in
    [ (match kind with `Rest -> "api-expires" | `Ws -> "api-nonce"), [Int.to_string nonce];
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

  let call ?buf ?log ?(span=Time_ns.Span.of_int_sec 1) ?(max_tries=3) ~name ~f uri =
    let rec inner_exn try_id =
      f uri >>= fun (resp, body) ->
      Body.to_string body >>= fun body_str ->
      let status = Response.status resp in
      let status_code = C.Code.code_of_status status in
      if C.Code.is_success status_code then return @@ Yojson.Safe.from_string ?buf body_str
      else if C.Code.is_client_error status_code then begin
        match error_of_yojson Yojson.Safe.(from_string ?buf body_str) with
        | Ok { error = { name; message }} -> failwithf "%s: %s" name message ()
        | Error _ -> failwithf "%s: json error" name ()
      end
      else if C.Code.is_server_error status_code then begin
        let status_code_str = (C.Code.sexp_of_status_code status |> Sexplib.Sexp.to_string_hum) in
        Option.iter log ~f:(fun log -> Log.error log "%s: %s" name status_code_str);
        Clock_ns.after span >>= fun () ->
        if try_id >= max_tries then failwithf "%s: %s" name status_code_str ()
        else inner_exn @@ succ try_id
      end
      else failwithf "%s: Unexpected HTTP return status %s" name (C.Code.sexp_of_status_code status |> Sexplib.Sexp.to_string_hum) ()
    in
    Monitor.try_with_or_error (fun () -> inner_exn 0)

  let mk_headers ?log ?(data="") ~key ~secret verb uri =
    let query_params = Crypto.mk_query_params ?log ~data ~key ~secret `Rest verb uri in
    Cohttp.Header.of_list @@
    ("content-type", "application/json") :: List.Assoc.map query_params ~f:List.hd_exn

  module ApiKey = struct
    type permission = [`Perm of string | `Dtc of string] [@@deriving sexp]

    let perms_of_raw = function
    | `String perm -> `Perm perm
    | `List [`String "dtc"; `Assoc ["username", `String username]] -> `Dtc username
    | #Yojson.Safe.json -> invalid_arg "perms_of_raw"

    type entry_raw = {
      id: string;
      secret: string;
      name: string;
      nonce: string;
      cidr: string;
      permissions: Yojson.Safe.json list;
      enabled: bool;
      userid: int;
      created: string;
    } [@@deriving yojson]

    type entry = {
      id: string;
      secret: string;
      name: string;
      nonce: string;
      cidr: string;
      permissions: permission list;
      enabled: bool;
      userid: int;
      created: Time_ns.t;
    } [@@deriving create, sexp]

    let entry_of_raw ({ id; secret; name; nonce; cidr; permissions; enabled; userid; created }:entry_raw) =
      let permissions = List.map permissions ~f:perms_of_raw in
      let created = Time_ns.of_string created in
      create_entry ~id ~secret ~name ~nonce ~cidr ~permissions ~enabled ~userid ~created ()

    let dtc ?buf ?log ?username ~testnet ~key ~secret () =
      let path = "/api/v1/apiKey/dtc/" ^ match username with None -> "all" | Some u -> "get" in
      let query = match username with None -> [] | Some u -> ["get", u] in
      let uri = if testnet then testnet_uri else uri in
      let uri = Uri.with_query' uri query in
      let uri = Uri.with_path uri path in
      let headers = mk_headers ?log ~key ~secret `GET uri in
      call ?buf ?log ~name:"position" ~f:(Client.get ~headers) uri >>| function
      | Ok (`List entries) -> Ok (List.map entries ~f:(fun e -> entry_raw_of_yojson e |> Result.ok_or_failwith |> entry_of_raw))
      | Ok json -> Error (Error.of_string (Yojson.Safe.to_string ?buf json))
      | Error err -> Error err
  end

  module Position = struct
    let position ?buf ?log ~testnet ~key ~secret () =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/position" in
      let headers = mk_headers ?log ~key ~secret `GET uri in
      call ?buf ?log ~name:"position" ~f:(Client.get ~headers) uri
  end

  module Order = struct
    let submit ?buf ?log ~testnet ~key ~secret orders =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/bulk" in
      let body_str = Yojson.Safe.to_string ?buf @@ `Assoc ["orders", `List orders] in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `POST uri in
      Option.iter log ~f:(fun log -> Log.debug log "-> %s" body_str);
      call ?buf ?log ~name:"submit" ~f:(Client.post ~chunked:false ~body ~headers) uri

    let update ?buf ?log ~testnet ~key ~secret orders =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/bulk" in
      let body_str = Yojson.Safe.to_string ?buf @@ `Assoc ["orders", `List orders] in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `PUT uri in
      Option.iter log ~f:(fun log -> Log.debug log "-> %s" body_str);
      call ?buf ?log ~name:"update" ~f:(Client.put ~chunked:false ~body ~headers) uri

    let cancel ?buf ?log ~testnet ~key ~secret orderID =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order" in
      let body_str = `Assoc ["orderID", `String Uuid.(to_string orderID)] |> Yojson.Safe.to_string in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `DELETE uri in
      Option.iter log ~f:(fun log -> Log.debug log "-> %s" body_str);
      call ?buf ?log ~name:"cancel" ~f:(Client.delete ~chunked:false ~body ~headers) uri

    let cancel_all ?buf ?log ?symbol ?filter ~testnet ~key ~secret () =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/all" in
      let body = `Assoc begin
          List.filter_opt
            [Option.map filter ~f:(fun json -> "filter", json);
             Option.map symbol ~f:(fun sym -> "symbol", `String sym);
            ]
        end
      in
      let body_str = Yojson.Safe.to_string ?buf body in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `DELETE uri in
      Option.iter log ~f:(fun log -> Log.debug log "-> %s" body_str);
      call ?buf ?log ~name:"cancel_all" ~f:(Client.delete ~chunked:false ~body ~headers) uri

    let cancel_all_after ?buf ?log ~testnet ~key ~secret timeout =
      let uri = Uri.with_path (if testnet then testnet_uri else uri) "/api/v1/order/cancelAllAfter" in
      let body_str = Yojson.Safe.to_string ?buf @@ `Assoc ["timeout", `Int timeout] in
      let body = Body.of_string body_str in
      let headers = mk_headers ?log ~key ~secret ~data:body_str `POST uri in
      Option.iter log ~f:(fun log -> Log.debug log "-> %s" body_str);
      call ?buf ?log ~name:"cancel_all_after" ~f:(Client.post ~chunked:false ~body ~headers) uri
  end
end

module Ws = struct
  module Yojson = struct
    module Safe = struct
      include Yojson.Safe
      let pp_json fmt json = Format.fprintf fmt "%s" @@ to_string json
      let show_json json = Format.(fprintf str_formatter "%a" pp_json json)
    end

    module Basic = struct
      include Yojson.Basic
    end
  end

  type request = {
    op: string;
    args: Yojson.Safe.json;
  } [@@deriving create, show, yojson]

  type error = {
    status: int [@default 0];
    error: string;
    meta: Yojson.Safe.json [@default `Null];
    request: request option [@default None];
  } [@@deriving yojson, show]

  type response = {
    success: bool;
    symbol: (string [@default ""]);
    subscribe: (string [@default ""]);
    account: (int [@default 0]);
    request: request;
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

  type msg =
    | Welcome
    | Ok of response
    | Error of error
    | Update of update

  let msg_of_yojson = function
  | (`Assoc fields) as json ->
    if List.Assoc.mem fields "info" && List.Assoc.mem fields "version" then Welcome
    else if List.Assoc.mem fields "error" then Error (error_of_yojson json |> Result.ok_or_failwith)
    else if List.Assoc.mem fields "success" then Ok (response_of_yojson json |> Result.ok_or_failwith)
    else if List.Assoc.mem fields "table" then Update (update_of_yojson json |> Result.ok_or_failwith)
    else invalid_argf "Ws.msg_of_yojson: %s" Yojson.Safe.(to_string json) ()
  | #Yojson.Safe.json as json -> invalid_argf "Ws.msg_of_yojson: %s" Yojson.Safe.(to_string json) ()

  module MD = struct
    type typ = Message | Subscribe | Unsubscribe [@@deriving enum]
    type t = {
      typ: typ;
      id: string;
      topic: string;
      payload: Yojson.Safe.json option;
    } [@@deriving create]

    let of_yojson = function
    | `List (`Int typ :: `String id :: `String topic :: payload) -> begin
        try
          let payload = match payload with
          | [] -> None
          | [payload] -> Some payload
          | _ -> raise Exit
          in
          Result.return @@ create
            ~typ:(Option.value_exn (typ_of_enum typ))
            ~id
            ~topic ?payload ()
        with _ -> Result.fail "MD.of_yojson"
      end
    | #Yojson.Safe.json -> Result.fail "MD.of_yojson"

    let to_yojson { typ; id; topic; payload } =
      let payload = match payload with None -> [] | Some p -> [p] in
      `List (`Int (typ_to_enum typ) :: `String id :: `String topic :: payload)

    let subscribe ~id ~topic = create Subscribe id topic ()
    let unsubscribe ~id ~topic = create Unsubscribe id topic ()
    let auth ~id ~topic ~key ~secret =
      let nonce, signature = Crypto.sign ~secret ~verb:`GET ~endp:"/realtime" `Ws in
      let payload = create_request ~op:"authKey" ~args:(`List [`String key; `Int nonce; `String signature]) () |> request_to_yojson in
      create ~typ:Message ~id ~topic ~payload ()
    let message ~id ~topic ~payload = create ~typ:Message ~id ~topic ~payload ()
  end

  let uri_of_opts testnet md =
    Uri.with_path
      (if testnet then testnet_uri else uri)
      (if md then "realtimemd" else "realtime")

  let open_connection ?connected ?(buf=Bi_outbuf.create 4096) ?to_ws ?(query_params=[]) ?log ?auth ~testnet ~md ~topics () =
    let uri = uri_of_opts testnet md in
    let auth_params = match auth with
      | None -> []
      | Some (key, secret) -> Crypto.mk_query_params ?log ~key ~secret `Ws `GET uri
    in
    let uri = Uri.add_query_param uri ("heartbeat", ["true"]) in
    let uri = Uri.add_query_params uri @@
      if md then [] else
      ["subscribe", topics] @ auth_params @ query_params
    in
    let uri_str = Uri.to_string uri in
    let host = Option.value_exn ~message:"no host in uri" Uri.(host uri) in
    let port = Option.value_exn ~message:"no port inferred from scheme"
        Uri_services.(tcp_port_of_uri uri)
    in
    let scheme = Option.value_exn ~message:"no scheme in uri" Uri.(scheme uri) in
    let ws_w_mvar = Mvar.create () in
    let rec loop_write mvar msg =
      let mvar_ro = Mvar.read_only mvar in
      Mvar.value_available mvar_ro >>= fun () ->
      let w = Mvar.peek_exn mvar_ro in
      if Pipe.is_closed w then begin
        Mvar.take mvar_ro >>= fun _ ->
        loop_write mvar msg
      end
      else Pipe.write w msg
    in
    Option.iter to_ws ~f:begin fun to_ws ->
      don't_wait_for @@
      Monitor.handle_errors begin fun () ->
        Pipe.iter ~continue_on_error:true to_ws ~f:begin fun msg_json ->
          let msg_str = Yojson.Safe.to_string msg_json in
          Option.iter log ~f:(fun log -> Log.debug log "-> %s" msg_str);
          loop_write ws_w_mvar msg_str
        end
      end
        (fun exn -> Option.iter log ~f:(fun log -> Log.error log "%s" @@ Exn.to_string exn))
    end;
    let client_r, client_w = Pipe.create () in
    let cleanup r w ws_r ws_w =
      Pipe.close_read ws_r;
      Deferred.all_unit [Reader.close r; Writer.close w]
    in
    let tcp_fun s r w =
      Socket.(setopt s Opt.nodelay true);
      (if scheme = "https" || scheme = "wss" then Conduit_async_ssl.ssl_connect r w else return (r, w)) >>= fun (r, w) ->
      let ws_r, ws_w = Websocket_async.client_ez ?log ~heartbeat:(Time_ns.Span.of_int_sec 25) uri s r w in
      Mvar.set ws_w_mvar ws_w;
      Option.iter connected ~f:(fun c -> Mvar.set c ());
      Option.iter log ~f:(fun log -> Log.info log "[WS] connecting to %s" uri_str);
      Monitor.protect ~finally:(fun () -> cleanup r w ws_r ws_w) (fun () -> Pipe.transfer ws_r client_w ~f:(Yojson.Safe.from_string ~buf))
    in
    let rec loop () = begin
      Monitor.try_with_or_error ~name:"with_connection" (fun () ->
          Tcp.(with_connection (to_host_and_port host port) tcp_fun)) >>| function
      | Ok () -> Option.iter log ~f:(fun log -> Log.error log "[WS] connection to %s terminated" uri_str);
      | Error err -> Option.iter log ~f:(fun log -> Log.error log "[WS] connection to %s raised %s" uri_str (Error.to_string_hum err))
    end >>= fun () ->
      if Pipe.is_closed client_r then Deferred.unit
      else begin
        Option.iter log ~f:(fun log -> Log.error log "[WS] restarting connection to %s" uri_str);
        Clock_ns.after @@ Time_ns.Span.of_int_sec 10 >>= loop
      end
    in
    don't_wait_for @@ loop ();
    client_r

  module Kaiko = struct
    type ticker = { last: string; bid: string; ask: string } [@@deriving yojson]

    type data = {
      timestamp: int;
      index: ticker;
      exchanges: Yojson.Safe.json [@default `Null];
    } [@@deriving yojson]

    type evt = {
      exchange: string;
      channel: string;
      data: data;
    } [@@deriving yojson]

    type exchange = {
      name: string;
      channels: string list;
    } [@@deriving create, yojson]

    type msg = {
      typ: string [@key "type"];
      exchanges: exchange list;
    } [@@deriving create, yojson]

    let subscribe exchanges =
      let exchanges = List.map exchanges ~f:begin fun name ->
          create_exchange ~name ~channels:["ticker"] ()
        end
      in
      create_msg ~typ:"subscribe" ~exchanges ()

    let tickers ?log exchanges =
      let buf = Bi_outbuf.create 1024 in
      let uri = Uri.of_string "https://markets.kaiko.com:8080/v1" in
      let uri_str = Uri.to_string uri in
      let host = Option.value_exn ~message:"no host in uri" Uri.(host uri) in
      let port = Option.value_exn ~message:"no port inferred from scheme"
          Uri_services.(tcp_port_of_uri uri)
      in
      let scheme = Option.value_exn ~message:"no scheme in uri" Uri.(scheme uri) in
      let client_r, client_w = Pipe.create () in
      let subscribe_msg_str = subscribe exchanges |> msg_to_yojson |> Yojson.Safe.to_string ~buf in
      let tcp_fun s r w =
        Socket.(setopt s Opt.nodelay true);
        begin
          if scheme = "https" || scheme = "wss" then Conduit_async_ssl.ssl_connect r w
          else return (r, w)
        end >>= fun (r, w) ->
        let ws_r, ws_w = Websocket_async.client_ez ?log ~heartbeat:(Time_ns.Span.of_int_sec 25) uri s r w in
        let cleanup () =
          Pipe.close_read ws_r;
          Deferred.all_unit [Reader.close r; Writer.close w]
        in
        Option.iter log ~f:(fun log -> Log.info log "[WS] connecting to %s" uri_str);
        Monitor.protect ~finally:cleanup begin fun () ->
          Pipe.write ws_w subscribe_msg_str >>= fun () ->
          Pipe.transfer' ws_r client_w ~f:begin fun msgq ->
            return @@ Queue.filter_map msgq ~f:begin fun msg_str ->
              Yojson.Safe.from_string ~buf msg_str |> evt_of_yojson |> function
              | Ok evt -> Some evt.data
              | Error msg ->
                Option.iter log ~f:(fun log -> Log.error log "%s: %s" msg msg_str);
                None
            end
          end
        end
      in
      let rec loop () = begin
        Monitor.try_with_or_error ~name:"with_connection" (fun () ->
            Tcp.(with_connection (to_host_and_port host port) tcp_fun)) >>| function
        | Ok () -> Option.iter log ~f:(fun log -> Log.error log "[WS] connection to %s terminated" uri_str);
        | Error err -> Option.iter log ~f:(fun log -> Log.error log "[WS] connection to %s raised %s" uri_str (Error.to_string_hum err))
      end >>= fun () ->
        if Pipe.is_closed client_r then Deferred.unit
        else begin
          Option.iter log ~f:(fun log -> Log.error log "[WS] restarting connection to %s" uri_str);
          Clock_ns.after @@ Time_ns.Span.of_int_sec 10 >>= loop
        end
      in
      don't_wait_for @@ loop ();
      client_r
  end
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
  | Dtc.OrderType.Market -> "Market"
  | Limit -> "Limit"
  | Stop -> "Stop"
  | Stop_limit -> "StopLimit"
  | Market_if_touched -> "MarketIfTouched"

let ord_type_of_string = function
  | "Market" -> Dtc.OrderType.Market
  | "Limit" -> Limit
  | "Stop" -> Stop
  | "StopLimit" -> Stop_limit
  | "MarketIfTouched" -> Market_if_touched
  | s -> invalid_argf "ord_type_of_string: %s" s ()

let string_of_tif = function
  | Dtc.TimeInForce.Day -> "Day"
  | Good_till_canceled
  | All_or_none -> "GoodTillCancel"
  | Immediate_or_cancel -> "ImmediateOrCancel"
  | Fill_or_kill -> "FillOrKill"
  | Good_till_date_time -> invalid_arg "string_of_tif: unsupported Good_till_date_time"

let tif_of_string = function
  | "Day" -> Dtc.TimeInForce.Day
  | "GoodTillCancel" -> Good_till_canceled
  | "ImmediateOrCancel" -> Immediate_or_cancel
  | "FillOrKill" -> Fill_or_kill
  | s -> invalid_argf "tif_of_string: %s" s ()

let p1_p2_of_bitmex ~ord_type ~stopPx ~price = match ord_type with
  | Dtc.OrderType.Market -> None, None
  | Limit -> Some price, None
  | Stop -> Some stopPx, None
  | Stop_limit -> Some stopPx, Some price
  | Market_if_touched -> Some stopPx, None

let string_of_stop_exec_inst = function
  | `MarkPrice -> "MarkPrice"
  | `LastPrice -> "LastPrice"
  | `IndexPrice -> "IndexPrice"

let price_fields_of_dtc ?p1 ?p2 ord_type =
  match ord_type with
  | Dtc.OrderType.Market -> []
  | Limit -> begin match p1 with
    | None -> []
    | Some p1 -> ["price", `Float p1]
    end
  | Stop | Market_if_touched -> begin match p1 with
    | None -> invalid_arg "price_field_of_dtc" (* Cannot happen *)
    | Some p1 -> ["stopPx", `Float p1]
    end
  | Stop_limit ->
    List.filter_opt [
      Option.map p1 ~f:(fun p1 -> "stopPx", `Float p1);
      Option.map p2 ~f:(fun p2 -> "price", `Float p2);
    ]

let execInst_of_dtc ord_type tif stop_exec_inst =
  let sei_str = string_of_stop_exec_inst stop_exec_inst in
  let execInst = match ord_type with
    | Dtc.OrderType.Stop | Market_if_touched | Stop_limit -> Some sei_str
    | _ -> None
  in
  match tif with
  | Dtc.TimeInForce.All_or_none -> [
      "execInst", `String (Option.value_map execInst ~default:"AllOrNone" ~f:(fun ei -> "AllOrNone," ^ ei));
      "displayQty", `Float 0.
    ]
  | _ -> Option.value_map execInst ~default:[] ~f:(fun ei -> ["execInst", `String ei])

let update_action_of_string = function
  | "partial" -> OB.Partial
  | "insert" -> Insert
  | "update"  -> Update
  | "delete" -> Delete
  | s -> invalid_argf "update_action_of_bitmex: %s" s ()

let side_of_bmex = function
  | "Buy" -> Ok Dtc.Buy
  | "Sell" -> Ok Sell
  | s -> Error s

let bmex_of_side = function
  | Dtc.Buy -> "Buy"
  | Sell -> "Sell"
