#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe "bs_api" @@ fun c ->
  Ok [
    Pkg.lib ~exts:Exts.library "src/bitmex_api";
    Pkg.lib ~exts:Exts.library "src/bfx_api";
    Pkg.lib ~exts:Exts.library "src/poloniex_api";
    Pkg.bin ~auto:true "src/ws";
    Pkg.mllib "src/bs_api.mllib"
  ]
