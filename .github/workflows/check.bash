v=$(bash get_version.bash) || exit
(($(git tag | grep "v$v" | wc -l) == 0)) || exit
