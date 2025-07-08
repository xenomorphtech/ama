defmodule Test do
  @moduledoc """
  Documentation for `Test`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Test.hello()
      :world

  """
  def hello do
    cf =
      [
        "tx_receiver_nonce|receiver:nonce->txhash",
        "tx_account_nonce|account:nonce->txhash",
        "sysconf",
        "muts_rev",
        "muts",
        "contractstate",
        "consensus_by_entryhash|Map<mutationshash,consensus>",
        "consensus",
        "my_attestation_for_entry|entryhash",
        "my_seen_time_entry|entryhash",
        "tx|txhash:entryhash",
        "entry_by_slot|slot:entryhash",
        "entry_by_height|height:entryhash"
      ]

    cf = cf |> Enum.map(&{&1, []})

    {:ok, db_ref, cfs} =
      RustlerRocksDB.open_optimistic_transaction_db(
        "/home/sdancer/projects/ama/db/fabric/",
        nil,
        cf
      )
  end

  def iter_all() do
    m = Enum.map(1..10_000_000, fn x -> RustlerRocksDB.iterator_next(it) end)
    Enum.each(m, fn {a, v} -> IO.puts(StringInspector.inspect_with_hex(a)) end)
  end
end
