# rqbench completion                                       -*- shell-script -*-

_rqbench_completions_filter() {
  local words="$1"
  local cur=${COMP_WORDS[COMP_CWORD]}
  local result=()

  if [[ "${cur:0:1}" == "-" ]]; then
    echo "$words"

  else
    for word in $words; do
      [[ "${word:0:1}" != "-" ]] && result+=("$word")
    done

    echo "${result[*]}"

  fi
}

_rqbench_completions() {
  local cur=${COMP_WORDS[COMP_CWORD]}
  local compwords=("${COMP_WORDS[@]:1:$COMP_CWORD-1}")
  local compline="${compwords[*]}"

  case "$compline" in
    '-t'*)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -W "https http" -- "$cur")
      ;;
    *)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -W "$(_rqbench_completions_filter "-a -b -m -n -o -q -t -x")" -- "$cur")
      ;;

  esac
} &&
  complete -F _rqbench_completions rqbench

# ex: filetype=sh
