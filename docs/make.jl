using Documenter, Feather

makedocs(
    modules = [Feather],
)

deploydocs(
    deps = Deps.pip("mkdocs", "python-markdown-math"),
    repo = "github.com/JuliaStats/Feather.jl.git"
)
