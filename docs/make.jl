using Documenter, Feather

makedocs(
    modules = [Feather],
    format = :html,
    sitename = "Feather.jl",
    pages = ["Home" => "index.md"]
)

deploydocs(
    repo = "github.com/JuliaData/Feather.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
    julia = "nightly",
    osname = "linux"
)
