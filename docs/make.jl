using Documenter, Feather

makedocs(
    modules = [Feather],
    format = :html,
    sitename = "Feather.jl",
    pages = ["Home" => "index.md"]
)

deploydocs(
    repo = "github.com/JuliaStats/Feather.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
    julia = "0.5",
    osname = "linux"
)
