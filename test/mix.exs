defmodule RustlerRocksDB.MixProject do

  use Mix.Project



  def project do

    [

      app: :rustler_rocksdb,

      version: "0.1.0",

      elixir: "~> 1.13",

      start_permanent: Mix.env() == :prod,

      deps: deps(),

      compilers: Mix.compilers() ++ [:rustler],

      rustler_crates: rustler_crates()

    ]

  end



  def application do

    [

      extra_applications: [:logger]

    ]

  end



  defp deps do

    [

      {:rustler, "~> 0.29.0"}

    ]

  end



  defp rustler_crates do

    [

      rustler_rocksdb: [

        path: "native/rustler_rocksdb",

        mode: if(Mix.env() == :prod, do: :release, else: :debug)

      ]

    ]

  end

end
