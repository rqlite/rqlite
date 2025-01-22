# rqlited completion                                       -*- shell-script -*-

_rqlited_completions_filter() {
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

_rqlited_completions() {
  local cur=${COMP_WORDS[COMP_CWORD]}
  local compwords=("${COMP_WORDS[@]:1:$COMP_CWORD-1}")
  local compline="${compwords[*]}"

  case "$compline" in
    '-node-verify-server-name'*)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -A hostname -- "$cur")
      ;;
    '-extensions-path' | \
    '-trace-profile' | \
    '-on-disk-path' | \
    '-auto-restore' | \
    '-disco-confie' | \
    '-http-ca-cert' | \
    '-node-ca-cert' | \
    '-auto-backup' | \
    '-cpu-profile' | \
    '-mem-profile' | \
    '-node-cert' | \
    '-http-cert' | \
    '-http-key' | \
    '-node-key' | \
    '-auth'*)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -A file -- "$cur")
      ;;
    *)
      while read -r; do COMPREPLY+=("$REPLY"); done < <(compgen -W "$(_rqlited_completions_filter "-auth -auto-backup -auto-optimize-int -auto-restore -auto-vacuum-int -bootstrap-expect -bootstrap-expect-timeout -cluster-connect-timeout -cpu-profile -disco-confie -disco-key -disco-mode -extensions-path -fk -http-addr -http-adv-addr -http-allow-origin -http-ca-cert -http-cert -http-key -http-verify-client -join -join-as -join-attempts -join-interval -mem-profile -node-ca-cert -node-cert -node-id -node-key -node-no-verify -node-verify-client -node-verify-server-name -on-disk-path -raft-addr -raft-adv-addr -raft-apply-timeout -raft-cluster-remove-shutdown -raft-election-timeout -raft-leader-lease-timeout -raft-log-level -raft-non-voter -raft-reap-node-timeout -raft-reap-read-only-node-timeout -raft-remove-shutdown -raft-shutdown-stepdown -raft-snap -raft-snap-int -raft-snap-wal-size -raft-timeout duration -trace-profile -version -write-queue-batch-size -write-queue-capacity -write-queue-timeout -write-queue-tx")" -- "$cur")
      ;;

  esac
} &&
  complete -F _rqlited_completions rqlited

# ex: filetype=sh
