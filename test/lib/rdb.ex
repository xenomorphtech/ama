defmodule RustlerRocksDB do
  use Rustler, otp_app: :rustler_rocksdb, crate: "rustler_rocksdb"

  def init(_db_path), do: :erlang.nif_error(:nif_not_loaded)

  def get(_key), do: :erlang.nif_error(:nif_not_loaded)

  def put(_key, _value), do: :erlang.nif_error(:nif_not_loaded)
end
