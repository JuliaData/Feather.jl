using Documenter, Feather

makedocs(;
    modules=[Feather],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/JuliaData/Feather.jl/blob/{commit}{path}#L{line}",
    sitename="Feather.jl",
    assets=String[],
)

deploydocs(;
    repo="github.com/JuliaData/Feather.jl",
    devbranch = "main"
)
