using Documenter, Feather

makedocs(
    modules = [Feather],
    format = :html,
    sitename = "Feather.jl",
    pages = ["Home" => "index.md"]
)

deploydocs(
    deps = Deps.pip("mkdocs", "python-markdown-math"),
    repo = "github.com/JuliaData/Feather.jl.git",
    julia = "1.0",
    osname = "linux"
)
