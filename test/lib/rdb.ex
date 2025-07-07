defmodule RustlerRocksDB do
  use Rustler, otp_app: :rustler_rocksdb, crate: "rustler_rocksdb"

  # --- Database Management ---

  @doc """
  Opens an OptimisticTransactionDB with specified global options and a list of column families.

  Each column family is defined as a tuple `{name, options}`.

  ## Options

  Database and Column Family options are passed as a keyword list. Supported keys:
  * `:create_if_missing` (boolean)
  * `:create_missing_column_families` (boolean)
  * `:target_file_size_base` (integer)
  * `:target_file_size_multiplier` (integer)

  ## Returns

  * `{:ok, db_resource, list_of_cf_names}` on success.
  * `{:error, reason}` on failure.
  """
  def open_optimistic_transaction_db(_path, _db_opts, _cf_descriptors),
    do: :erlang.nif_error(:nif_not_loaded)

  # --- Direct DB Operations ---

  @doc "Puts a key-value pair into a specific column family."
  def put_cf(_db_resource, _cf_name, _key, _value), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Gets a value by key from a specific column family.

  Returns `{:ok, value}` or `{:ok, nil}` if the key is not found.
  """
  def get_cf(_db_resource, _cf_name, _key), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Deletes a key from a specific column family."
  def delete_cf(_db_resource, _cf_name, _key), do: :erlang.nif_error(:nif_not_loaded)

  # --- Transaction Management ---

  @doc "Begins a new optimistic transaction. Returns `{:ok, transaction_resource}`."
  def begin_transaction(_db_resource), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Commits a transaction. The transaction resource cannot be used after this call."
  def commit_transaction(_transaction_resource), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Rolls back a transaction. The transaction resource cannot be used after this call."
  def rollback_transaction(_transaction_resource), do: :erlang.nif_error(:nif_not_loaded)

  # --- Transactional Operations ---

  @doc "Puts a key-value pair into a column family within a transaction."
  def transaction_put_cf(_transaction_resource, _cf_name, _key, _value),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc "Gets a value by key from a column family within a transaction."
  def transaction_get_cf(_transaction_resource, _cf_name, _key),
    do: :erlang.nif_error(:nif_not_loaded)

  # --- Iterator Management ---

  @doc """
  Creates a new iterator over a specific column family.

  The iterator starts at the beginning of the column family.

  Returns `{:ok, iterator_resource}`.
  """
  def iterator_cf(_db_resource, _cf_name), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Moves the iterator to the next key-value pair.

  ## Returns

  * `{:ok, {key, value}}` if the next item exists.
  * `:finished` if the iterator has reached the end.
  * `{:error, reason}` on a database error.
  """
  def iterator_next(_iterator_resource), do: :erlang.nif_error(:nif_not_loaded)
end
