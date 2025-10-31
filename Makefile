emit:
\tpython -m emitter.emit_stdout --rate 5 --max 10
emit-realistic:
\tpython -m emitter.emit_stdout --rate 10 --max 200 --jitter 0.2 --burst-prob 0.04
emit-fast:
\tpython -m emitter.emit_stdout --rate 20 --max 50
replay-ulb:
\tpython -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop
replay-ulb-realistic:
\tpython -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --jitter 0.2 --burst-prob 0.04