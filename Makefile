emit:
\tpython -m emitter.emit_stdout --rate 5 --max 10
emit-fast:
\tpython -m emitter.emit_stdout --rate 20 --max 50
replay-ulb:
\tpython -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop