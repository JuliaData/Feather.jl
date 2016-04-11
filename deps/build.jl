using BinDeps

#@windows_only push!(BinDeps.defaults, SimpleBuild)

@BinDeps.setup

lib_prefix = @windows ? "" : "lib"
lib_suffix = @windows ? "dll" : (@osx? "dylib" : "so")

genopt = "Unix Makefiles"
@windows_only begin
	if WORD_SIZE == 64
		genopt = "Visual Studio 14 2015 Win64"
	else
		genopt = "Visual Studio 14 2015"
	end
end

featherjl = library_dependency("featherjl", aliases=["libfeatherjl"])

prefix = joinpath(BinDeps.depsdir(featherjl), "usr")
featherjl_srcdir = joinpath(BinDeps.depsdir(featherjl), "src")
featherjl_builddir = joinpath(BinDeps.depsdir(featherjl), "builds")
provides(BuildProcess,
	(@build_steps begin
		CreateDirectory(featherjl_builddir)
		@build_steps begin
			ChangeDirectory(featherjl_builddir)
			FileRule(joinpath(prefix, "lib", "$(lib_prefix)featherjl.$lib_suffix"), @build_steps begin
				`cmake -G "$genopt" -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_BUILD_TYPE="Release" $featherjl_srcdir`
				`cmake --build . --config Release --target install`
			end)
		end
	end), featherjl)

deps = [featherjl]
#provides(Binaries, Dict(URI("https://github.com/JuliaStats/Feather.jl/releases/download/v0.1.2/CxxWrap.zip") => deps), os = :Windows)

@BinDeps.install

@windows_only pop!(BinDeps.defaults)
