open Core.Std
open Async.Std
open Log.Global

open Websocket_async
open Bs_devkit.Core
open Bitmex_api

let client key secret testnet topics =
  let url = Uri.of_string (if testnet then "https://testnet.bitmex.com" else "https://www.bitmex.com") in
  let host = Option.value_exn ~message:"no host in uri" Uri.(host url) in
  let port = Option.value_exn ~message:"no port inferred from scheme"
      Uri_services.(tcp_port_of_uri url) in
  let scheme = Option.value_exn ~message:"no scheme in uri" Uri.(scheme url) in
  let read_line_and_write_to_pipe w =
    let rec loop () =
      Reader.(read_line Lazy.(force stdin)) >>= function
      | `Eof ->
          debug "Got EOF. Closing pipe.";
          Pipe.close w;
          Shutdown.exit 0
      | `Ok s -> Pipe.write w s >>= loop
    in loop ()
  in
  let tcp_fun s r w =
    Socket.(setopt s Opt.nodelay true);
    (if scheme = "https" || scheme = "wss"
     then Conduit_async_ssl.ssl_connect r w
     else return (r, w)) >>= fun (r, w) ->
    let nonce, signature =
      Crypto.sign secret `GET "/realtime" `Ws
    in
    let url =
    Uri.(add_query_params (with_path url "/realtime")
           [ "heartbeat", ["true"];
             "subscribe", topics;
             "api-nonce", [nonce];
             "api-key", [key];
             "api-signature", [signature];
           ])
    in
    let r, w = client_ez
        ~log:Lazy.(force log)
        ~heartbeat:Time.Span.(of_sec 25.) url s r w
    in
    don't_wait_for @@ read_line_and_write_to_pipe w;
    Pipe.transfer r Writer.(pipe @@ Lazy.force stderr) ~f:(fun s -> s ^ "\n")
  in
  Tcp.(with_connection (to_host_and_port host port) tcp_fun)

let command =
  let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu" in
  let spec =
    let open Command.Spec in
    empty
    +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of cfg (default: ~/.virtu)"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-loglevel" (optional int) ~doc:"1-3 loglevel"
    +> anon (sequence ("topic" %: string))
  in
  let set_loglevel = function
    | 2 -> set_level `Info
    | 3 -> set_level `Debug
    | _ -> ()
  in
  let run cfg testnet loglevel topics =
    let exchange = "BMEX" ^ (if testnet then "T" else "") in
    let cfg_json = Yojson.Safe.from_file cfg in
    let cfg = Cfg.of_yojson cfg_json |> presult_exn in
    let { Cfg.key; secret } = List.Assoc.find_exn cfg exchange in
    let secret = Cstruct.of_string secret in
    Option.iter loglevel ~f:set_loglevel;
    don't_wait_for @@ client key secret testnet topics;
    never_returns @@ Scheduler.go ()
  in
  Command.basic ~summary:"Test interface to Websockets" spec run

let () = Command.run command
