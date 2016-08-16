using Documenter, Feather

makedocs(
    modules = [Feather],
)

deploydocs(
    deps = Deps.pip("mkdocs", "mkdocs-material", "python-markdown-math"),
    repo = "github.com/JuliaStats/Feather.jl.git"
)
