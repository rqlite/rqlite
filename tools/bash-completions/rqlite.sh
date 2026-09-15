# rqlite completion                                        -*- shell-script -*-

_rqlite_completions_filter() {
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

_rqlite_completions() {
  local cur=${COMP_WORDS[COMP_CWORD]}
  local compwords=("${COMP_WORDS[@]:1:$COMP_CWORD-1}")
  local compline="${compwords[*]}"

  case "$compline" in
    '--client-cert'* | '--client-key'* | '--ca-cert'* | '-c'* | '-d'* | '-k'*)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -A file -- "$cur")
      ;;
    '--host'* | '-H'*)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -A hostname -- "$cur")
      ;;
    '--scheme'* | '-s'*)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -W "https http" -- "$cur")
      ;;
    *)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -W "$(_rqlite_completions_filter "-h --help -a --alternatives -s --scheme -H --host -p --port -P --prefix -i --insecure -c --ca-cert -d --client-cert -k --client-key -u --user -v --version -t --http-timeout")" -- "$cur")
      ;;

  esac
} &&
  complete -F _rqlite_completions rqlite

# ex: filetype=sh
