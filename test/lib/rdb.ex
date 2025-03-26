defmodule RustlerRocksDB do

  use Rustler, otp_app: :rustler_rocksdb, crate: "rustler_rocksdb"



    # When your NIF is loaded, it will override these functions

      def init(_db_path), do: :erlang.nif_error(:nif_not_loaded)

        def get(_key), do: :erlang.nif_error(:nif_not_loaded)

          def put(_key, _value), do: :erlang.nif_error(:nif_not_loaded)

          end
